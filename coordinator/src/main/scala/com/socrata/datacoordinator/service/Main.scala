package com.socrata.datacoordinator.service

import java.util.UUID

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.simplearm.v2._
import com.rojoma.simplearm.v2.conversions._
import com.socrata.datacoordinator.common.soql.SoQLRep
import com.socrata.datacoordinator.common.collocation.{CollocationLock, CuratedCollocationLock, NoOPCollocationLock}
import com.socrata.datacoordinator.common.{DataSourceFromConfig, SoQLCommon}
import com.socrata.datacoordinator.id.{ColumnId, DatasetId, DatasetInternalName, UserColumnId}
import com.socrata.datacoordinator.resources._
import com.socrata.datacoordinator.secondary.{DatasetAlreadyInSecondary, SecondaryMetric}
import com.socrata.datacoordinator.secondary.config.SecondaryGroupConfig
import com.socrata.datacoordinator.truth.CopySelector
import com.socrata.datacoordinator.truth.loader.{Delogger, NullLogger}
import com.socrata.datacoordinator.truth.universe.sql.PostgresUniverse
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.util.collection.UserColumnIdSet
import com.socrata.datacoordinator.util._
import com.socrata.http.common.AuxiliaryData
import com.socrata.http.server._
import com.socrata.http.server.curator.CuratorBroker
import com.socrata.http.server.livenesscheck.LivenessCheckResponder
import com.socrata.http.server.util.{EntityTag, Precondition}
import com.socrata.curator.{CuratorFromConfig, DiscoveryFromConfig, ProviderCache}
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.typesafe.config.{Config, ConfigFactory}

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import com.socrata.datacoordinator.resources.collocation._
import com.socrata.datacoordinator.service.Main.log
import com.socrata.datacoordinator.service.collocation.{MetricProvider, _}
import com.socrata.http.client.HttpClientHttpClient
import org.apache.curator.x.discovery.{ServiceInstanceBuilder, strategies}
import org.apache.log4j.PropertyConfigurator
import org.joda.time.DateTime

import scala.util.Random

class Main(common: SoQLCommon, serviceConfig: ServiceConfig) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Main])

  def ensureInSecondary(coordinator: Coordinator)(storeId: String, datasetId: DatasetId): Boolean = {
    val remote = Set.newBuilder[DatasetInternalName]

    val result =
      for(u <- common.universe) {
        def loop(datasetId: DatasetId): Boolean = {
          try {
            u.datasetMapWriter.datasetInfo(datasetId, serviceConfig.writeLockTimeout) match {
              case Some(_) =>
                // ok so this also needs to keep collocation
                // up-to-date, which means we need to find out what
                // datasets are collocated with it, and add them too.
                // That may entail telling other DCs to move their
                // datasets.  There's no danger of loops because this
                // "add dataset" step will fail and we'll just say "ok
                // all is well" if it's already here.
                u.secondaryManifest.addDataset(storeId, datasetId)
                // great, now do collocation stuff...
                val collocatedDatasets = u.collocationManifest.collocatedDatasets(Set(common.internalNameFromDatasetId(datasetId)))
                for(otherInternalName <- collocatedDatasets) {
                  common.datasetIdFromInternalName(otherInternalName) match {
                    case Some(otherDatasetId) =>
                      loop(otherDatasetId)
                    case None =>
                      // lives on another DC, we'll have to poke it...
                      DatasetInternalName(otherInternalName) match {
                        case Some(dsin) =>
                          remote += dsin
                        case None =>
                          log.warn("Collocation query returned an invalid internal name: {}", JString(otherInternalName))
                      }
                  }
                }
                true
              case None =>
                log.info("No dataset found with id: {}", datasetId)
                false
            }
          } catch {
            case _: DatasetAlreadyInSecondary =>
              // ok, it's there
              true
          }
        }
        loop(datasetId)
      }

    for(remoteDataset <- remote.result()) {
      coordinator.ensureInSecondary(storeId, remoteDataset)
    }

    result
  }

  def ensureInSecondaryGroup(coordinator: Coordinator, collocationProvider: CollocatorProvider)(secondaryGroupStr: String, datasetId: DatasetId, secondariesLike: Option[DatasetInternalName]): Boolean = {
    for(u <- common.universe) {
      val secondaryGroup = serviceConfig.secondary.groups.getOrElse(secondaryGroupStr, {
        log.info("Can't find secondary group {}", secondaryGroupStr)
        return false
      }
      )

      val currentDatasetSecondaries = secondariesOfDataset(datasetId).map(_.secondaries.keySet).getOrElse(Set.empty)

      val newSecondaries = secondariesToAdd(collocationProvider, secondaryGroup,
        currentDatasetSecondaries,
        datasetId,
        secondaryGroupStr,
        secondariesLike)

      newSecondaries.toVector.map(ensureInSecondary(coordinator)(_, datasetId)).forall(identity) // no side effects in forall
    }
  }

  def deleteFromSecondary(storeId: String, datasetId: DatasetId): Boolean =
    for(u <- common.universe) {
      u.datasetMapWriter.datasetInfo(datasetId, serviceConfig.writeLockTimeout) match {
        case Some(_) =>
          log.info("Marking dataset {} for pending drop from store {}", datasetId, storeId)
          val found = u.secondaryManifest.markDatasetForDrop(storeId, datasetId)
          if (!found) log.info("No secondary manifest entry for dataset {} and store {}", datasetId, storeId)
          u.commit()
          found
        case None =>
          log.info("No dataset found with id: {}", datasetId)
          false
      }
    }

  def datasetsInStore(storeId: String): Map[DatasetId, Long] = {
    for (u <- common.universe) {
      u.secondaryManifest.datasets(storeId)
    }
  }

  def versionInStore(storeId: String, datasetId: DatasetId): Option[Long] = {
    for (u <- common.universe) {
      for {
        result <- u.secondaryManifest.readLastDatasetInfo(storeId, datasetId)
      } yield result._1
    }
  }

  def secondariesOfDataset(datasetId: DatasetId): Option[SecondariesOfDatasetResult] = {
    for (u <- common.universe) {
      val datasetMapReader = u.datasetMapReader
      datasetMapReader.datasetInfo(datasetId).map { datasetInfo =>
        val secondaryManifest = u.secondaryManifest
        val secondaryStoresConfig = u.secondaryStoresConfig
        val latestVersion = datasetMapReader.latest(datasetInfo).dataVersion

        val copies = datasetMapReader.allCopies(datasetInfo)
        val publishedVersion = copies.find { _.lifecycleStage == LifecycleStage.Published }.map { _.dataVersion }
        val unpublishedVersion = copies.find { _.lifecycleStage == LifecycleStage.Unpublished }.map { _.dataVersion }


        val secondaries = secondaryManifest.stores(datasetId)
        val feedbackSecondaries = secondaryManifest.feedbackSecondaries(datasetId)
        val brokenSecondaries = secondaryManifest.brokenAts(datasetId)

        val groups = scala.collection.mutable.HashMap[String, Set[String]]()
        secondaries.keys.foreach { storeId =>
          secondaryStoresConfig.group(storeId).foreach { group =>
            groups += ((group, groups.getOrElse(group, Set.empty) ++ Set(storeId)))
          }
        }

        SecondariesOfDatasetResult(
          serviceConfig.instance,
          latestVersion,
          latestVersion,
          publishedVersion,
          unpublishedVersion,
          secondaries,
          feedbackSecondaries,
          groups.toMap,
          brokenSecondaries
        )
      }
    }
  }

  def secondaryMoveJobs(jobId: UUID): SecondaryMoveJobsResult = {
    for (u <- common.universe) {
      SecondaryMoveJobsResult(u.secondaryMoveJobs.jobs(jobId))
    }
  }

  def secondaryMoveJobs(storeGroup: String, datasetId: DatasetId): Either[ResourceNotFound, SecondaryMoveJobsResult] = {
    serviceConfig.secondary.groups.get(storeGroup) match {
      case Some(groupConfig) =>
        for (u <- common.universe) {
          u.datasetMapReader.datasetInfo(datasetId) match {
            case Some(_) =>
              val moves = u.secondaryMoveJobs.jobs(datasetId).filter { move => groupConfig.instances.keySet(move.toStoreId) }

              Right(SecondaryMoveJobsResult(moves))
            case None =>
              Left(DatasetNotFound(common.datasetInternalNameFromDatasetId(datasetId)))
          }
        }
      case None => Left(StoreGroupNotFound(storeGroup))
    }
  }


  def ensureSecondaryMoveJob(coordinator: Coordinator)
                            (storeGroup: String,
                             datasetId: DatasetId,
                             request: SecondaryMoveJobRequest): Either[ResourceNotFound, Either[InvalidMoveJob, Boolean]] = {
    serviceConfig.secondary.groups.get(storeGroup) match {
      case Some(groupConfig) =>
        for (u <- common.universe) {
          u.datasetMapReader.datasetInfo(datasetId) match {
            case Some(_) =>
              val fromStoreId = request.fromStoreId
              val toStoreId = request.toStoreId

              if (!groupConfig.instances.keySet(fromStoreId)) return Left(StoreNotFound(fromStoreId))
              if (!groupConfig.instances.keySet(toStoreId)) return Left(StoreNotFound(toStoreId))

              if (!groupConfig.instances(toStoreId).acceptingNewDatasets) {
                log.warn("Cannot move dataset to store {} not accepting new datasets", toStoreId)
                return Right(Left(StoreNotAcceptingDatasets))
              }

              val currentStores =
                secondariesOfDataset(datasetId).map(_.secondaries.keySet).getOrElse(Set.empty).
                  filter(groupConfig.instances.keySet(_))

              if(currentStores(toStoreId)) {
                // it's already in the target store.  We don't need to do anything!
                Right(Right(false))
              } else {
                val existingJobs = u.secondaryMoveJobs.jobs(datasetId)

                // apply moves to the current stores in the order they were created in
                val futureStores = existingJobs.sorted.foldLeft(currentStores) { (stores, move) =>
                  stores - move.fromStoreId + move.toStoreId
                }

                log.info("Told to move {} from {} to {}", datasetId.asInstanceOf[AnyRef], fromStoreId, toStoreId)
                log.info("Current stores: {}", currentStores)
                log.info("Future stores: {}", futureStores)

                val result = if (futureStores(fromStoreId) && !currentStores(toStoreId)) {
                  // once existing moves complete the dataset will be "from" this store
                  // and the dataset will not yet be "to" the other store:
                  // so we should add a new move job

                  try {
                    ensureInSecondary(coordinator)(toStoreId, datasetId)
                  } catch {
                    case _: DatasetAlreadyInSecondary => ???
                  }

                  u.secondaryMoveJobs.addJob(request.jobId, datasetId, fromStoreId, toStoreId)

                  Right(true)
                } else if (!futureStores(fromStoreId) && currentStores(toStoreId)) {
                  // there is an existing (not complete) job doing the same thing
                  existingJobs.find { job => job.fromStoreId == fromStoreId && job.toStoreId == toStoreId } match {
                    case Some(existingJob) => Right(false)
                    case None => ???
                  }
                } else {
                  // in this case it does not make sense to move the dataset from
                  // one store to the other
                  Left(DatasetNotInStore)
                }

                Right(result)
              }
            case None => Left(DatasetNotFound(common.datasetInternalNameFromDatasetId(datasetId)))
          }
        }
      case None => Left(StoreGroupNotFound(storeGroup))
    }
  }

  def rollbackSecondaryMoveJob(datasetId: DatasetId,
                               request: SecondaryMoveJobRequest,
                               dropFromStore: Boolean): Option[DatasetNotFound] = {
    for (u <- common.universe) {
      u.datasetMapWriter.datasetInfo(datasetId, serviceConfig.writeLockTimeout) match {
        case Some(_) =>
          u.secondaryMoveJobs.deleteJob(request.jobId, datasetId, request.fromStoreId, request.toStoreId)

          try {
            u.secondaryManifest.addDataset(request.fromStoreId, datasetId)
          } catch {
            case _: DatasetAlreadyInSecondary => // that's fine
          }

          if (dropFromStore) u.secondaryManifest.markDatasetForDrop(request.toStoreId, datasetId)

          None
        case None =>
          Some(DatasetNotFound(common.datasetInternalNameFromDatasetId(datasetId)))
      }
    }
  }

  def collocatedDatasets(datasets: Set[DatasetInternalName]): CollocatedDatasetsResult = {
    for (u <- common.universe) {
      val collocatedDatasets = u.collocationManifest.collocatedDatasets(datasets.map(_.underlying)).map { dataset =>
        DatasetInternalName(dataset).getOrElse {
          // we will validate dataset internal names on the way in so this should never happen... but
          log.error("collocation-manifest contains an invalid dataset internal name: {}", dataset)
          throw new Exception(s"collocation-manifest contains an invalid dataset internal name: $dataset")
        }
      }
      CollocatedDatasetsResult(collocatedDatasets)
    }
  }

  def addCollocations(jobId: UUID, collocations: Seq[(DatasetInternalName, DatasetInternalName)]): Unit = {
    for (u <- common.universe) {
      u.collocationManifest.addCollocations(jobId, collocations.map { case (l, r) => (l.underlying, r.underlying) }.toSet)
    }
  }

  def dropCollocations(dataset: DatasetInternalName): Unit = {
    for (u <- common.universe) {
      u.collocationManifest.dropCollocations(dataset.underlying)
    }
  }

  def dropCollocationJob(jobId: UUID): Unit = {
    for (u <- common.universe) {
      u.collocationManifest.dropCollocations(jobId)
      u.secondaryMoveJobs.deleteJob(jobId)
    }
  }

  def secondaryMetrics(storeId: String, datasetId: Option[DatasetId]): Either[ResourceNotFound, Option[SecondaryMetric]] = {
    val secondaries = serviceConfig.secondary.groups.flatMap(_._2.instances.keySet).toSet
    if (secondaries(storeId)) {
      for (u <- common.universe) {
        datasetId match {
          case Some(id) => u.datasetMapReader.datasetInfo(id) match {
            case Some(_) => Right(u.secondaryMetrics.dataset(storeId, id))
            case None => Left(DatasetNotFound(common.datasetInternalNameFromDatasetId(id)))
          }
          case None => Right(Some(u.secondaryMetrics.storeTotal(storeId)))
        }
      }
    } else {
      Left(StoreNotFound(storeId))
    }
  }

  private def mutator(tmp: IndexedTempFile) = new Mutator(tmp, common.Mutator)

  def processMutation(datasetId: DatasetId, input: Iterator[JValue], tmp: IndexedTempFile) = {
    for(u <- common.universe) {
      mutator(tmp).updateScript(u, datasetId, input)
    }
  }

  def processCreation(input: Iterator[JValue], tmp: IndexedTempFile) = {
    for(u <- common.universe) {
      mutator(tmp).createScript(u, input)
    }
  }

  def listDatasets(): Seq[DatasetId] = {
    for(u <- common.universe) {
      u.datasetMapReader.allDatasetIds()
    }
  }

  def deleteDataset(datasetId: DatasetId) = {
    for(u <- common.universe) {
      u.datasetDropper.dropDataset(datasetId)
    }
  }

  def exporter(
    id: DatasetId,
    schemaHash: Option[String],
    copy: CopySelector,
    columns: Option[UserColumnIdSet],
    limit: Option[Long],
    offset: Option[Long],
    precondition: Precondition,
    ifModifiedSince: Option[DateTime],
    sorted: Boolean,
    rowId: Option[String]
   )(f: Either[Schema, (EntityTag, Long, DateTime, Seq[SchemaField], Option[UserColumnId],
     String,
     Long,
     Iterator[Array[JValue]])] => Unit): Exporter.Result[Unit] = {
    for(u <- common.universe) {
      readRowId(u, id, copy, rowId) match {
        case Right(rid) =>
          Exporter.export(u, id, copy, columns, limit, offset, precondition, ifModifiedSince, sorted, rid)
          { (entityTag, copyCtx, approxRowCount, it) =>
            val schema = u.schemaFinder.getSchema(copyCtx)

            if(schemaHash.isDefined && (Some(schema.hash) != schemaHash)) {
              f(Left(schema))
            } else {
              val jsonReps = common.jsonReps(copyCtx.datasetInfo)
              val jsonSchema = copyCtx.schema.mapValuesStrict { ci => jsonReps(ci.typ) }
              val unwrappedCids = copyCtx.schema.values.toSeq.filter { ci => jsonSchema.contains(ci.systemId) }
                .sortBy(_.userColumnId).map(_.systemId.underlying).toArray
              val pkColName = copyCtx.pkCol.map(_.userColumnId)
              val orderedSchema = unwrappedCids.map { cidRaw =>
                val col = copyCtx.schema(new ColumnId(cidRaw))
                SchemaField(
                  col.userColumnId,
                  col.fieldName,
                  col.computationStrategyInfo.map(CompStratSchemaField.convert),
                  col.typ.name.name
                )
              }
              f(Right((
                entityTag,
                copyCtx.datasetInfo.latestDataVersion,
                copyCtx.copyInfo.lastModified,
                orderedSchema,
                pkColName,
                copyCtx.datasetInfo.localeName,
                approxRowCount,
                it.map { row =>
                  val arr = new Array[JValue](unwrappedCids.length)
                  var i = 0
                  while(i != unwrappedCids.length) {
                    val cid = new ColumnId(unwrappedCids(i))
                    val rep = jsonSchema(cid)
                    arr(i) = rep.toJValue(row(cid))
                    i += 1
                  }
                  arr
                }))
              )
            }
          }
        case Left(err) => err
      }
    }
  }

  /**
   * Read row identifier value in string form into SoQLValue.
   * It is an error only when the column rep cannot parse a non-empty row id value.
   * It is not an error if row id value does not exist but is valid.
   */
  private def readRowId(u: PostgresUniverse[SoQLType, SoQLValue],
                        id: DatasetId,
                        copy: CopySelector,
                        rowId: Option[String]): Either[Exporter.Result[Unit], Option[SoQLValue]] = {

    ( rowId.flatMap { rid =>
       // Missing dataset copy is not handled as error here.  It is handled further downstream.
       for(ctxOpt <- u.datasetReader.openDataset(id, copy)) {
         for(ctx <- ctxOpt.toOption) yield {
           ctx.copyCtx.userIdCol match {
             case Some(_) => // dataset has custom row identifier
               val rowIdRep = SoQLRep.csvRep(ctx.copyCtx.pkCol_!)
               // optional row id type must be simple which is represented by a single csv string.
               rowIdRep.decode(IndexedSeq(rid), IndexedSeq(0))
                 .map(x => Right(Some(x)))
                 .getOrElse(Left(Exporter.InvalidRowId))
             case None => // no customer row identifier.  Use system row identifier.
               val rowIdRep = common.jsonReps(ctx.copyCtx.datasetInfo)(ctx.copyCtx.pkCol_!.typ)
               rowIdRep.fromJValue(JString(rid)).map(x => Right(Some(x))).getOrElse(Left(Exporter.InvalidRowId))
           }
         }
       }
     }
    ).getOrElse(Right(None))
  }

  def makeReportTemporaryFile() =
    new IndexedTempFile(
      indexBufSizeHint = serviceConfig.reports.indexBlockSize,
      dataBufSizeHint = serviceConfig.reports.dataBlockSize,
      tmpDir = serviceConfig.reports.directory)

  def secondariesToAdd(collocationProvider: CollocatorProvider, secondaryGroup: SecondaryGroupConfig, currentDatasetSecondaries: Set[String],
                       datasetId: DatasetId, secondaryGroupStr: String, secondariesLike: Option[DatasetInternalName]): Set[String] = {

    val preferredSecondaries = secondariesLike match {
      case Some(datasetInternalName) =>
        val candidateSecondariesInGroup = secondaryGroup.instances.filter(_._2.acceptingNewDatasets).keySet
        val secs = collocationProvider.collocator.secondariesOfDataset(datasetInternalName)
        secs.find(!candidateSecondariesInGroup.contains(_)) match {
          case Some(sec) =>
            throw new Exception(s"Secondary ${sec} like ${datasetInternalName.underlying} is no longer accepting datasets" )
          case None =>
            secs
        }
      case None =>
        Set.empty[String]
    }

    Main.secondariesToAdd(secondaryGroup, currentDatasetSecondaries, datasetId, secondaryGroupStr, preferredSecondaries)
  }
}

object Main extends DynamicPortMap {
  lazy val log = org.slf4j.LoggerFactory.getLogger(classOf[Service])

  val configRoot = "com.socrata.coordinator.service"

  def withDefaultAddress(config: Config): Config = {
    val ifaces = ServiceInstanceBuilder.getAllLocalIPs
    if(ifaces.isEmpty) config
    else {
      val first = JString(ifaces.iterator.next().getHostAddress)
      val addressConfig = ConfigFactory.parseString(s"$configRoot.service-advertisement.address=" + first)
      config.withFallback(addressConfig)
    }
  }

  def main(args: Array[String]) {
    val serviceConfig = try {
      new ServiceConfig(withDefaultAddress(ConfigFactory.load()), configRoot, hostPort)
    } catch {
      case e: Exception =>
        Console.err.println(e)
        sys.exit(1)
    }

    PropertyConfigurator.configure(Propertizer("log4j", serviceConfig.logProperties))

    val secondaries: Set[String] = serviceConfig.secondary.groups.flatMap(_._2.instances.keySet).toSet
    // TODO: remove this
    val secondariesNotAcceptingNewDatasets: Set[String] =
      serviceConfig.secondary.groups.flatMap { case (_, group) =>
        group.instances.filter { case (_, instance) => !instance.acceptingNewDatasets }.keySet
      }.toSet

    val collocationGroup: Set[String] = serviceConfig.collocation.group
    if (collocationGroup.nonEmpty && !collocationGroup.contains(serviceConfig.instance)) {
      throw new Exception(s"Instance self ${serviceConfig.instance} is required to be in the collocation group when non-empty: $collocationGroup")
    }

    implicit val executorShutdown = Resource.executorShutdownNoTimeout

    for {
      dsInfo <- DataSourceFromConfig(serviceConfig.dataSource)
      executorService <- managed(Executors.newCachedThreadPool)
      httpClient <- managed(new HttpClientHttpClient(executorService))
    } {
      val common = locally {
        // force RT to be initialized to avoid circular-dependency NPE
        // Merely putting a reference to T is sufficient; the call to hashCode
        // is to silence a compiler warning about ignoring a pure value
        clojure.lang.RT.T.hashCode

        // serviceConfig.tablespace must be an expression with the free variable
        // table-name which should return either `nil' or a valid Postgresql tablespace
        // name.
        //
        // I wish there were an obvious way to read the expression separately and then
        // splice it in knowing that it's well-formed.
        val iFn = clojure.lang.Compiler.load(new java.io.StringReader(s"""(let [op (fn [^String table-name] ${serviceConfig.tablespace})]
                                                                            (fn [^String table-name]
                                                                              (let [result (op table-name)]
                                                                                (if result
                                                                                    (scala.Some. result)
                                                                                    (scala.Option/empty)))))""")).asInstanceOf[clojure.lang.IFn]
        new SoQLCommon(
          dsInfo.dataSource,
          dsInfo.copyIn,
          executorService,
          { t => iFn.invoke(t).asInstanceOf[Option[String]] },
          new DebugLoggedTimingReport(org.slf4j.LoggerFactory.getLogger("timing-report")) with StackedTimingReport,
          allowDdlOnPublishedCopies = serviceConfig.allowDdlOnPublishedCopies,
          serviceConfig.writeLockTimeout,
          serviceConfig.instance,
          serviceConfig.reports.directory,
          serviceConfig.logTableCleanupDeleteOlderThan,
          serviceConfig.logTableCleanupDeleteEvery,
          //serviceConfig.tableCleanupDelay,
          NullCache
        )
      }

      val operations = new Main(common, serviceConfig)

      def getSchema(datasetId: DatasetId) = {
        for(u <- common.universe) {
          for(dsInfo <- u.datasetMapReader.datasetInfo(datasetId)) yield {
            val latest = u.datasetMapReader.latest(dsInfo)
            val schema = u.datasetMapReader.schema(latest)
            val ctx = new DatasetCopyContext(latest, schema)
            u.schemaFinder.getSchema(ctx)
          }
        }
      }

      def getSnapshots(datasetId: DatasetId) = {
        for(u <- common.universe) {
          for(dsInfo <- u.datasetMapReader.datasetInfo(datasetId)) yield {
            u.datasetMapReader.snapshots(dsInfo).map(_.unanchored).toVector
          }
        }
      }

      def deleteSnapshot(datasetId: DatasetId, copyNum: Long): CopyContextResult[UnanchoredCopyInfo] = {
        for(u <- common.universe) {
          val dsInfo = u.datasetMapWriter.datasetInfo(datasetId, common.PostgresUniverseCommon.writeLockTimeout).getOrElse {
            return CopyContextResult.NoSuchDataset
          }
          u.datasetMapWriter.snapshots(dsInfo).find(_.copyNumber == copyNum) match {
            case None =>
              CopyContextResult.NoSuchCopy
            case Some(snapshot) =>
              u.schemaLoader(NullLogger[u.CT, u.CV]).drop(snapshot) // NullLogger because DropSnapshot is no longer a visible thing
              u.datasetMapWriter.dropCopy(snapshot)
              CopyContextResult.CopyInfo(snapshot.unanchored)
          }
        }
      }

      def getLog(datasetId: DatasetId, version: Long)(f: Iterator[Delogger.LogEvent[common.CV]] => Unit) = {
        for {
          u <- common.universe
          dsInfo <- u.datasetMapReader.datasetInfo(datasetId)
        } {
          using(u.delogger(dsInfo).delog(version)) { it =>
            f(it)
          }
        }
      }

      def getRollups(datasetId: DatasetId) = {
        for(u <- common.universe) {
          for(dsInfo <- u.datasetMapReader.datasetInfo(datasetId)) yield {
            val latest = u.datasetMapReader.latest(dsInfo)
            u.datasetMapReader.rollups(latest).toSeq
          }
        }
      }

      def getSnapshottedDatasets() = {
        for(u <- common.universe) {
          u.datasetMapReader.snapshottedDatasets()
        }
      }


      val notFoundDatasetResource = NotFoundDatasetResource(_: Option[String], common.internalNameFromDatasetId,
        operations.makeReportTemporaryFile _, operations.processCreation,
        operations.listDatasets _, _: (=> HttpResponse) => HttpResponse, serviceConfig.commandReadLimit)
      val datasetSchemaResource = DatasetSchemaResource(_: DatasetId, getSchema, common.internalNameFromDatasetId)
      val datasetSnapshotsResource = DatasetSnapshotsResource(_: DatasetId, getSnapshots, common.internalNameFromDatasetId)
      val datasetSnapshotResource = DatasetSnapshotResource(_: DatasetId, _: Long, deleteSnapshot, common.internalNameFromDatasetId)
      val datasetLogResource = DatasetLogResource[common.CV](_: DatasetId, _: Long, getLog, common.internalNameFromDatasetId)
      val datasetRollupResource = DatasetRollupResource(_: DatasetId, getRollups, common.internalNameFromDatasetId)
      val snapshottedResource = SnapshottedResource(getSnapshottedDatasets _, common.internalNameFromDatasetId)
      val secondaryManifestsResource = SecondaryManifestsResource(_: Option[String], secondaries,
        operations.datasetsInStore, common.internalNameFromDatasetId)

      implicit val weightedCostOrdering: Ordering[Cost] = WeightedCostOrdering(
        movesWeight = serviceConfig.collocation.cost.movesWeight,
        totalSizeBytesWeight = serviceConfig.collocation.cost.totalSizeBytesWeight,
        moveSizeMaxBytesWeight = serviceConfig.collocation.cost.moveSizeMaxBytesWeight
      )

      def httpCoordinator(hostAndPort: HostAndPort): Coordinator =
        new HttpCoordinator(
          serviceConfig.instance.equals,
          serviceConfig.secondary.defaultGroups,
          serviceConfig.secondary.groups,
          hostAndPort,
          httpClient, // since executorService is currently unbounded we can share here
          operations.collocatedDatasets,
          operations.dropCollocations,
          operations.secondariesOfDataset,
          operations.ensureInSecondary,
          operations.secondaryMetrics,
          operations.secondaryMoveJobs,
          operations.secondaryMoveJobs,
          operations.ensureSecondaryMoveJob,
          operations.rollbackSecondaryMoveJob
        )

      def collocationProvider(hostAndPort: HostAndPort,
                              lock: CollocationLock): CoordinatorProvider with
                                                      CollocatorProvider with
                                                      MetricProvider = {
        new CoordinatorProvider(httpCoordinator(hostAndPort)) with CollocatorProvider with MetricProvider {
          override val metric: Metric = CoordinatedMetric(collocationGroup, coordinator)
          override val collocator: Collocator = new CoordinatedCollocator(
            collocationGroup = collocationGroup,
            coordinator = coordinator,
            metric = metric,
            addCollocations = operations.addCollocations,
            lock = lock,
            lockTimeoutMillis = serviceConfig.collocation.lockTimeout.toMillis
          )
        }
      }

      def metricProvider(hostAndPort: HostAndPort): CoordinatorProvider with MetricProvider = {
        new CoordinatorProvider(httpCoordinator(hostAndPort)) with MetricProvider {
          override val metric: Metric = CoordinatedMetric(collocationGroup, coordinator)
        }
      }

      def datasetResource(lock: CollocationLock)(hostAndPort: String => Option[(String, Int)]) = {
        DatasetResource(
          _: DatasetId,
          operations.makeReportTemporaryFile _,
          serviceConfig.commandReadLimit,
          operations.processMutation,
          operations.deleteDataset,
          operations.exporter,
          collocationProvider(hostAndPort, lock).collocator,
          _: (=> HttpResponse) => HttpResponse,
          common.internalNameFromDatasetId
        )
      }

      def secondaryManifestsCollocateResource(lock: CollocationLock)(hostAndPort: HostAndPort) = {
        SecondaryManifestsCollocateResource(_: String, collocationProvider(hostAndPort, lock))
      }

      val secondaryManifestsMetricsResource = SecondaryManifestsMetricsResource(_: String, _: Option[DatasetId],
        operations.secondaryMetrics, common.internalNameFromDatasetId)

      def secondaryManifestsMoveResource(hostAndPort: HostAndPort) = SecondaryManifestsMoveResource(
        _: Option[String],
        _: DatasetId,
        operations.secondaryMoveJobs,
        operations.ensureSecondaryMoveJob(httpCoordinator(hostAndPort)),
        operations.rollbackSecondaryMoveJob,
        common.datasetInternalNameFromDatasetId
      )

      def secondaryManifestsMoveJobResource(hostAndPort: HostAndPort) = SecondaryManifestsMoveJobResource(
        _: String,
        _: String,
        metricProvider(hostAndPort)
      )

      val secondaryMoveJobsJobResource = SecondaryMoveJobsJobResource(_: String, operations.secondaryMoveJobs, operations.dropCollocationJob)

      def collocationManifestsResource(lock: CollocationLock)(hostAndPort: HostAndPort) = {
        CollocationManifestsResource(_: Option[String], _: Option[String], collocationProvider(hostAndPort, lock))
      }

      def datasetSecondaryStatusResource(hostAndPort: HostAndPort) =
        DatasetSecondaryStatusResource(_: Option[String], _:DatasetId, secondaries,
                                       secondariesNotAcceptingNewDatasets, operations.versionInStore, serviceConfig, operations.ensureInSecondary(httpCoordinator(hostAndPort)),
                                       operations.ensureInSecondaryGroup(httpCoordinator(hostAndPort), collocationProvider(hostAndPort, NoOPCollocationLock)), operations.deleteFromSecondary, common.internalNameFromDatasetId)

      val secondariesOfDatasetResource = SecondariesOfDatasetResource(_: DatasetId, operations.secondariesOfDataset,
        common.internalNameFromDatasetId)

      val serv = Service(serviceConfig = serviceConfig,
        formatDatasetId = common.internalNameFromDatasetId,
        parseDatasetId = common.datasetIdFromInternalName,
        notFoundDatasetResource = notFoundDatasetResource,
        datasetResource = datasetResource,
        datasetSchemaResource = datasetSchemaResource,
        datasetSnapshotsResource = datasetSnapshotsResource,
        datasetSnapshotResource = datasetSnapshotResource,
        datasetLogResource = datasetLogResource,
        datasetRollupResource = datasetRollupResource,
        snapshottedResource = snapshottedResource,
        secondaryManifestsResource = secondaryManifestsResource,
        secondaryManifestsCollocateResource = secondaryManifestsCollocateResource,
        secondaryManifestsMetricsResource = secondaryManifestsMetricsResource,
        secondaryManifestsMoveResource = secondaryManifestsMoveResource,
        secondaryManifestsMoveJobResource = secondaryManifestsMoveJobResource,
        secondaryMoveJobsJobResource = secondaryMoveJobsJobResource,
        collocationManifestsResource = collocationManifestsResource,
        datasetSecondaryStatusResource = datasetSecondaryStatusResource,
        secondariesOfDatasetResource = secondariesOfDatasetResource) _

      val finished = new CountDownLatch(1)
      val tableDropper = new Thread() {
        setName("table dropper")
        override def run() {
          do {
            try {
              for(u <- common.universe) {
                while(finished.getCount > 0 && u.tableCleanup.cleanupPendingDrops()) {
                  u.commit()
                }
              }
            } catch {
              case e: Exception =>
                log.error("Unexpected error while dropping tables", e)
            }
          } while(!finished.await(30, TimeUnit.SECONDS))
        }
      }

      val logTableCleanup = new Thread() {
        setName("logTableCleanup thread")
        override def run() {
          do {
            try {
              for (u <- common.universe) {
                while (finished.getCount > 0 && u.logTableCleanup.cleanupOldVersions()) {
                  u.commit()
                  // a simple knob to allow us to slow the log table cleanup down without
                  // requiring complicated things.
                  finished.await(serviceConfig.logTableCleanupSleepTime.toMillis, TimeUnit.MILLISECONDS)
                }
              }
            } catch {
              case e: Exception =>
                log.error("Unexpected error while cleaning log tables", e)
            }
          } while(!finished.await(30, TimeUnit.SECONDS))
        }
      }

      try {
        tableDropper.start()
        logTableCleanup.start()
        val address = serviceConfig.discovery.address
        for {
          curator <- CuratorFromConfig(serviceConfig.curator)
          discovery <- DiscoveryFromConfig(classOf[AuxiliaryData], curator, serviceConfig.discovery)
          pong <- managed(new LivenessCheckResponder(serviceConfig.livenessCheck))
          provider <- managed(new ProviderCache(discovery, new strategies.RoundRobinStrategy, serviceConfig.discovery.name))
        } {
          pong.start()
          val auxData = new AuxiliaryData(livenessCheckInfo = Some(pong.livenessCheckInfo))

          def hostAndPort(instanceName: String): Option[(String, Int)] = {
            Option(provider(instanceName).getInstance()).map[(String, Int)](instance => (instance.getAddress, instance.getPort))
          }

          val collocationLockPath =  s"/${serviceConfig.discovery.name}/${serviceConfig.collocation.lockPath}"
          val collocationLock = new CuratedCollocationLock(curator, collocationLockPath)
          serv(collocationLock, hostAndPort).run(serviceConfig.network.port,
                   new CuratorBroker(discovery,
                                     address,
                                     serviceConfig.discovery.name + "." + serviceConfig.instance,
                                     Some(auxData)) {
                     override def register(port: Int): Cookie = {
                       super.register(hostPort(port))
                     }
                   }
                  )
        }
      } finally {
        finished.countDown()
      }

      log.info("Waiting for table dropper to terminate")
      tableDropper.join()
    }
  }

  def secondariesToAdd(secondaryGroup: SecondaryGroupConfig, currentDatasetSecondaries: Set[String],
                       datasetId: DatasetId, secondaryGroupStr: String, preferredSecondaries: Set[String] = Set.empty): Set[String] = {

    /*
     * The dataset may be in secondaries defined in other groups, but here we need to reason
     * only about secondaries in this group since selection is done group by group.  For example,
     * if we need two replicas in this group then secondaries outside this group don't count.
     */
    val currentDatasetSecondariesForGroup = currentDatasetSecondaries.intersect(secondaryGroup.instances.keySet)
    val desiredCopies = secondaryGroup.numReplicas
    val newCopiesRequired = Math.max(desiredCopies - currentDatasetSecondariesForGroup.size, 0)
    val candidateSecondariesInGroup = secondaryGroup.instances.filter(_._2.acceptingNewDatasets).keySet

    log.info(s"Dataset $datasetId exists on ${currentDatasetSecondariesForGroup.size} secondaries in group, " +
      s"want it on $desiredCopies so need to find $newCopiesRequired new secondaries")

    val newSecondaries =
      if (preferredSecondaries.nonEmpty) preferredSecondaries
      else Random.shuffle((candidateSecondariesInGroup -- currentDatasetSecondariesForGroup).toList)
                 .take(newCopiesRequired)
                 .toSet

    if (newSecondaries.size < newCopiesRequired) {
      // TODO: proper error, this is configuration error though
      throw new Exception(s"Can't find $desiredCopies servers in secondary group $secondaryGroupStr to publish to")
    }

    log.info(s"Dataset $datasetId should also be on $newSecondaries")

    newSecondaries
  }
}
