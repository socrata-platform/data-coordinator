package com.socrata.datacoordinator.service

import com.rojoma.simplearm.util._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.PropertyConfigurator
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.socrata.datacoordinator.secondary.{DatasetAlreadyInSecondary, SecondaryLoader}
import java.util.concurrent.{CountDownLatch, TimeUnit, Executors}
import com.socrata.datacoordinator.common.{SoQLCommon, StandardDatasetMapLimits, DataSourceFromConfig}
import com.socrata.datacoordinator.util.{NullCache, IndexedTempFile, StackedTimingReport, LoggedTimingReport}
import com.socrata.datacoordinator.id.{UserColumnId, ColumnId, DatasetId}
import com.rojoma.json.ast.{JString, JValue}
import com.socrata.datacoordinator.truth.CopySelector
import com.socrata.soql.environment.ColumnName
import org.apache.curator.retry
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.x.discovery.{ServiceInstanceBuilder, ServiceInstance, ServiceDiscoveryBuilder}
import com.socrata.http.server.curator.CuratorBroker
import com.rojoma.simplearm.SimpleArm
import java.net.{InetSocketAddress, InetAddress}
import com.socrata.datacoordinator.util.collection.UserColumnIdSet
import com.socrata.http.common.AuxiliaryData
import com.socrata.http.server.livenesscheck.LivenessCheckResponder
import com.socrata.datacoordinator.truth.metadata.{SchemaField, Schema, DatasetCopyContext}
import com.socrata.http.server.util.{EntityTag, Precondition}
import scala.util.Random
import com.socrata.datacoordinator.service.mutator.{DMLMutator, DDLMutator, UniversalMutator}
import org.joda.time.DateTime

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
    val currentDatasetSecondaries = secondariesOfDataset(datasetId).keySet
    for(u <- common.universe) {
      val secondaryGroup = serviceConfig.secondary.groups.get(secondaryGroupStr).getOrElse(
        // TODO: proper error
        throw new Exception(s"Can't find secondary group ${secondaryGroupStr}")
      )
      val desiredCopies: Int = secondaryGroup.numReplicas
      val newCopiesRequired: Int = Math.max(desiredCopies - currentDatasetSecondaries.size, 0)
      val secondariesInGroup: Set[String] = secondaryGroup.instances

      log.info(s"Dataset ${datasetId} exists on ${currentDatasetSecondaries.size} secondaries, want it on ${desiredCopies} so need ${newCopiesRequired} new secondaries")

      val newSecondaries = Random.shuffle((secondariesInGroup -- currentDatasetSecondaries).toList).take(newCopiesRequired)

      if (newSecondaries.size < newCopiesRequired) {
        // TODO: proper error
        throw new Exception(s"Can't find ${desiredCopies} servers in secondary group ${secondaryGroupStr} to publish to")
      }

      log.info(s"Publishing dataset ${datasetId} to ${newSecondaries}")

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

  def exporter(id: DatasetId, schemaHash: Option[String], copy: CopySelector, columns: Option[UserColumnIdSet], limit: Option[Long], offset: Option[Long], precondition: Precondition, sorted: Boolean)(f: Either[Schema, (EntityTag, Seq[SchemaField], Option[UserColumnId], String, Long, Iterator[Array[JValue]])] => Unit): Exporter.Result[Unit] = {
    for(u <- common.universe) yield {
      Exporter.export(u, id, copy, columns, limit, offset, precondition, sorted) { (entityTag, copyCtx, approxRowCount, it) =>
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
          f(Right(
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
            })
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

  def withDefaultAddress(config: Config): Config = {
    val ifaces = ServiceInstanceBuilder.getAllLocalIPs
    if(ifaces.isEmpty) config
    else {
      val first = JString(ifaces.iterator.next().getHostAddress)
      val addressConfig = ConfigFactory.parseString("com.socrata.coordinator.service.service-advertisement.address=" + first)
      config.withFallback(addressConfig)
    }
  }

  def main(args: Array[String]) {
    val serviceConfig = try {
      new ServiceConfig(withDefaultAddress(ConfigFactory.load()), "com.socrata.coordinator.service")
    } catch {
      case e: Exception =>
        Console.err.println(e)
        sys.exit(1)
    }

    PropertyConfigurator.configure(Propertizer("log4j", serviceConfig.logProperties))

    val secondaries = serviceConfig.secondary.instances.keySet

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

        val universalMutator = new UniversalMutator {
          val ddlMutator = new DDLMutator(common.Mutator)
          val dmlMutator = new DMLMutator(common.Mutator)

          override def createScript(commandStream: Iterator[JValue]): (DatasetId, Long, DateTime, Seq[mutator.MutationScriptCommandResult]) = {
            for(u <- common.universe) yield {
              ddlMutator.createScript(u, commandStream)
            }
          }

          override def upsertUtf8(datasetId: DatasetId, commandStream: Iterator[JValue]): Managed[(Long, DateTime, Iterator[Array[Byte]])] =
            new SimpleArm[(Long, DateTime, Iterator[Array[Byte]])] {
              override def flatMap[A](f: ((Long, DateTime, Iterator[Array[Byte]])) => A): A = {
                for(u <- common.universe) yield {
                  dmlMutator.upsertScript(u, datasetId, commandStream).flatMap(f)
                }
              }
            }

          override def copyOp(datasetId: DatasetId, command: JValue): (Long, DateTime) = {
            for(u <- common.universe) yield {
              ddlMutator.copyOp(u, datasetId, command)
            }
          }

          override def ddlScript(datasetId: DatasetId, commandStream: Iterator[JValue]): (Long, DateTime, Seq[mutator.MutationScriptCommandResult]) = {
            for(u <- common.universe) yield {
              ddlMutator.updateScript(u, datasetId, commandStream)
            }
          }
        }
        val serv = new Service(serviceConfig, operations.processMutation, operations.processCreation, getSchema _,
          operations.exporter, secondaries, operations.datasetsInStore, operations.versionInStore,
          operations.ensureInSecondary, operations.ensureInSecondaryGroup, operations.secondariesOfDataset, operations.listDatasets, operations.deleteDataset,
          serviceConfig.commandReadLimit, common.internalNameFromDatasetId, common.datasetIdFromInternalName, operations.makeReportTemporaryFile)

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
          val address = serviceConfig.advertisement.address
          for {
            curator <- managed(CuratorFrameworkFactory.builder.
              connectString(serviceConfig.curator.ensemble).
              sessionTimeoutMs(serviceConfig.curator.sessionTimeout.toMillis.toInt).
              connectionTimeoutMs(serviceConfig.curator.connectTimeout.toMillis.toInt).
              retryPolicy(new retry.BoundedExponentialBackoffRetry(serviceConfig.curator.baseRetryWait.toMillis.toInt,
              serviceConfig.curator.maxRetryWait.toMillis.toInt,
              serviceConfig.curator.maxRetries)).
              namespace(serviceConfig.curator.namespace).
              build())
            discovery <- managed(ServiceDiscoveryBuilder.builder(classOf[AuxiliaryData]).
              client(curator).
              basePath(serviceConfig.advertisement.basePath).
              build())
            pong <- managed(new LivenessCheckResponder(new InetSocketAddress(InetAddress.getByName(address), 0)))
          } {
            curator.start()
            discovery.start()
            pong.start()
            val auxData = new AuxiliaryData(livenessCheckInfo = Some(pong.livenessCheckInfo))
            serv.run(serviceConfig.network.port, new CuratorBroker(discovery, address, serviceConfig.advertisement.name + "." + serviceConfig.instance, Some(auxData)))
          }
        } finally {
          finished.countDown()
        }

        log.info("Waiting for table dropper to terminate")
        tableDropper.join()
      } finally {
        executorService.shutdown()
      }
      executorService.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
    }
  }
}
