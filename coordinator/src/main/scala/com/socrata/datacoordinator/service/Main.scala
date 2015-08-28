package com.socrata.datacoordinator.service

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.simplearm.SimpleArm
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.common.{SoQLCommon, StandardDatasetMapLimits, DataSourceFromConfig}
import com.socrata.datacoordinator.id.{UserColumnId, ColumnId, DatasetId}
import com.socrata.datacoordinator.secondary.{DatasetAlreadyInSecondary, SecondaryLoader}
import com.socrata.datacoordinator.truth.CopySelector
import com.socrata.datacoordinator.truth.metadata.{SchemaField, Schema, DatasetCopyContext}
import com.socrata.datacoordinator.util.collection.UserColumnIdSet
import com.socrata.datacoordinator.util.{NullCache, IndexedTempFile, StackedTimingReport, LoggedTimingReport}
import com.socrata.http.common.AuxiliaryData
import com.socrata.http.server.curator.CuratorBroker
import com.socrata.http.server.livenesscheck.LivenessCheckResponder
import com.socrata.http.server.util.{EntityTag, Precondition}
import com.socrata.soql.environment.ColumnName
import com.socrata.thirdparty.curator.{CuratorFromConfig, DiscoveryFromConfig}
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.typesafe.config.{Config, ConfigFactory}
import java.net.{InetSocketAddress, InetAddress}
import java.util.concurrent.{CountDownLatch, TimeUnit, Executors}
import org.apache.curator.x.discovery.{ServiceInstanceBuilder, ServiceInstance}
import org.apache.log4j.PropertyConfigurator
import org.joda.time.DateTime
import scala.util.Random

class Main(common: SoQLCommon, serviceConfig: ServiceConfig) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Main])

  def ensureInSecondary(storeId: String, datasetId: DatasetId): Unit =
    for(u <- common.universe) {
      try {
        u.datasetMapWriter.datasetInfo(datasetId, serviceConfig.writeLockTimeout) match {
          case Some(_) =>
            u.secondaryManifest.addDataset(storeId, datasetId)
          case None =>
            ??? // TODO: proper error
        }
      } catch {
        case _: DatasetAlreadyInSecondary =>
        // ok, it's there
      }
    }

  def ensureInSecondaryGroup(secondaryGroupStr: String, datasetId: DatasetId): Unit = {
    for(u <- common.universe) {
      val secondaryGroup = serviceConfig.secondary.groups.getOrElse(secondaryGroupStr,
        // TODO: proper error
        throw new Exception(s"Can't find secondary group $secondaryGroupStr")
      )

      val currentDatasetSecondaries = secondariesOfDataset(datasetId).keySet

      val newSecondaries = Main.secondariesToAdd(secondaryGroup,
        currentDatasetSecondaries,
        datasetId,
        secondaryGroupStr)

      newSecondaries.foreach(ensureInSecondary(_, datasetId))
    }
  }

  def datasetsInStore(storeId: String): Map[DatasetId, Long] =
    for(u <- common.universe) yield {
      u.secondaryManifest.datasets(storeId)
    }

  def versionInStore(storeId: String, datasetId: DatasetId): Option[Long] =
    for(u <- common.universe) yield {
      for {
        result <- u.secondaryManifest.readLastDatasetInfo(storeId, datasetId)
      } yield result._1
    }

  def secondariesOfDataset(datasetId: DatasetId): Map[String, Long] =
    for(u <- common.universe) yield {
      val secondaryManifest = u.secondaryManifest
      secondaryManifest.stores(datasetId)
    }

  private def mutator(tmp: IndexedTempFile) = new Mutator(tmp, common.Mutator)

  def processMutation(datasetId: DatasetId, input: Iterator[JValue], tmp: IndexedTempFile) = {
    for(u <- common.universe) yield {
      mutator(tmp).updateScript(u, datasetId, input)
    }
  }

  def processCreation(input: Iterator[JValue], tmp: IndexedTempFile) = {
    for(u <- common.universe) yield {
      mutator(tmp).createScript(u, input)
    }
  }

  def listDatasets(): Seq[DatasetId] = {
    for(u <- common.universe) yield {
      u.datasetMapReader.allDatasetIds()
    }
  }

  def deleteDataset(datasetId: DatasetId) = {
    for(u <- common.universe) yield {
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
    sorted: Boolean
   )(f: Either[Schema, (EntityTag, Seq[SchemaField], Option[UserColumnId], String, Long, Iterator[Array[JValue]])] => Unit): Exporter.Result[Unit] = {
    for(u <- common.universe) yield {
      Exporter.export(u, id, copy, columns, limit, offset, precondition, ifModifiedSince, sorted) { (entityTag, copyCtx, approxRowCount, it) =>
        val schema = u.schemaFinder.getSchema(copyCtx)

        if(schemaHash.isDefined && (Some(schema.hash) != schemaHash)) {
          f(Left(schema))
        } else {
          val jsonReps = common.jsonReps(copyCtx.datasetInfo)
          val jsonSchema = copyCtx.schema.mapValuesStrict { ci => jsonReps(ci.typ) }
          val unwrappedCids = copyCtx.schema.values.toSeq.filter { ci => jsonSchema.contains(ci.systemId) }.sortBy(_.userColumnId).map(_.systemId.underlying).toArray
          val pkColName = copyCtx.pkCol.map(_.userColumnId)
          val orderedSchema = unwrappedCids.map { cidRaw =>
            val col = copyCtx.schema(new ColumnId(cidRaw))
            SchemaField(col.userColumnId, col.typ.name.name)
          }
          f(Right((
            entityTag,
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
    }
  }

  def makeReportTemporaryFile() =
    new IndexedTempFile(
      indexBufSizeHint = serviceConfig.reports.indexBlockSize,
      dataBufSizeHint = serviceConfig.reports.dataBlockSize,
      tmpDir = serviceConfig.reports.directory)
}

object Main {
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
      new ServiceConfig(withDefaultAddress(ConfigFactory.load()), configRoot)
    } catch {
      case e: Exception =>
        Console.err.println(e)
        sys.exit(1)
    }

    PropertyConfigurator.configure(Propertizer("log4j", serviceConfig.logProperties))

    val secondaries: Set[String] = serviceConfig.secondary.groups.flatMap(_._2.instances).toSet

    for(dsInfo <- DataSourceFromConfig(serviceConfig.dataSource)) {
      val executorService = Executors.newCachedThreadPool()
      try {
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
            new LoggedTimingReport(org.slf4j.LoggerFactory.getLogger("timing-report")) with StackedTimingReport,
            allowDdlOnPublishedCopies = serviceConfig.allowDdlOnPublishedCopies,
            serviceConfig.writeLockTimeout,
            serviceConfig.instance,
            serviceConfig.reports.directory,
            serviceConfig.logTableCleanupDeleteOlderThan,
            serviceConfig.logTableCleanupDeleteEvery,
            NullCache
          )
        }

        val operations = new Main(common, serviceConfig)

        def getSchema(datasetId: DatasetId) = {
          for {
            u <- common.universe
            dsInfo <- u.datasetMapReader.datasetInfo(datasetId)
          } yield {
            val latest = u.datasetMapReader.latest(dsInfo)
            val schema = u.datasetMapReader.schema(latest)
            val ctx = new DatasetCopyContext(latest, schema)
            u.schemaFinder.getSchema(ctx)
          }
        }

        def getRollups(datasetId: DatasetId) = {
          for {
            u <- common.universe
            dsInfo <- u.datasetMapReader.datasetInfo(datasetId)
          } yield {
            val latest = u.datasetMapReader.latest(dsInfo)
            u.datasetMapReader.rollups(latest).toSeq
          }
        }

        val serv = new Service(serviceConfig, operations.processMutation, operations.processCreation, getSchema, getRollups,
          operations.exporter, secondaries, operations.datasetsInStore, operations.versionInStore,
          operations.ensureInSecondary, operations.ensureInSecondaryGroup, operations.secondariesOfDataset, operations.listDatasets, operations.deleteDataset,
          serviceConfig.commandReadLimit, common.internalNameFromDatasetId, common.datasetIdFromInternalName, operations.makeReportTemporaryFile)

        val finished = new CountDownLatch(1)
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
          logTableCleanup.start()
          val address = serviceConfig.discovery.address
          for {
            curator <- CuratorFromConfig(serviceConfig.curator)
            discovery <- DiscoveryFromConfig(classOf[AuxiliaryData], curator, serviceConfig.discovery)
            pong <- managed(new LivenessCheckResponder(serviceConfig.livenessCheck))
          } {
            pong.start()
            val auxData = new AuxiliaryData(livenessCheckInfo = Some(pong.livenessCheckInfo))
            serv.run(serviceConfig.network.port,
                     new CuratorBroker(discovery,
                                       address,
                                       serviceConfig.discovery.name + "." + serviceConfig.instance,
                                       Some(auxData)))
          }
        } finally {
          finished.countDown()
        }
      } finally {
        executorService.shutdown()
      }
      executorService.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
    }
  }


  def secondariesToAdd(secondaryGroup: SecondaryGroupConfig, currentDatasetSecondaries: Set[String],
                       datasetId: DatasetId, secondaryGroupStr: String): Set[String] = {

    /*
     * The dataset may be in secondaries defined in other groups, but here we need to reason 
     * only about secondaries in this group since selection is done group by group.  For example,
     * if we need two replicas in this group then secondaries outside this group don't count.
     */
    val currentDatasetSecondariesForGroup = currentDatasetSecondaries.intersect(secondaryGroup.instances)
    val desiredCopies = secondaryGroup.numReplicas
    val newCopiesRequired = Math.max(desiredCopies - currentDatasetSecondariesForGroup.size, 0)
    val secondariesInGroup = secondaryGroup.instances

    log.info(s"Dataset ${datasetId} exists on ${currentDatasetSecondariesForGroup.size} secondaries in group, want it on ${desiredCopies} so need to find ${newCopiesRequired} new secondaries")

    val newSecondaries = Random.shuffle((secondariesInGroup -- currentDatasetSecondariesForGroup).toList).take(newCopiesRequired).toSet

    if (newSecondaries.size < newCopiesRequired) {
      // TODO: proper error, this is configuration error though
      throw new Exception(s"Can't find ${desiredCopies} servers in secondary group ${secondaryGroupStr} to publish to")
    }

    log.info(s"Dataset ${datasetId} should also be on ${newSecondaries}")

    newSecondaries
  }
}
