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
import com.netflix.curator.retry
import com.netflix.curator.framework.CuratorFrameworkFactory
import com.netflix.curator.x.discovery.{ServiceInstanceBuilder, ServiceInstance, ServiceDiscoveryBuilder}
import com.socrata.http.server.curator.CuratorBroker
import com.rojoma.simplearm.SimpleArm
import java.net.{InetSocketAddress, InetAddress}
import com.socrata.datacoordinator.util.collection.UserColumnIdSet
import com.socrata.http.common.AuxiliaryData
import com.socrata.http.server.livenesscheck.LivenessCheckResponder
import com.socrata.datacoordinator.truth.metadata.{SchemaField, Schema, DatasetCopyContext}
import com.socrata.http.server.util.{EntityTag, Precondition}

class Main(common: SoQLCommon, serviceConfig: ServiceConfig) {
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

  def exporter(id: DatasetId, schemaHash: Option[String], copy: CopySelector, columns: Option[UserColumnIdSet], limit: Option[Long], offset: Option[Long], precondition: Precondition)(f: Either[Schema, (EntityTag, Seq[SchemaField], Option[UserColumnId], String, Iterator[Array[JValue]])] => Unit): Exporter.Result[Unit] = {
    for(u <- common.universe) yield {
      Exporter.export(u, id, copy, columns, limit, offset, precondition) { (entityTag, copyCtx, it) =>
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

    println(serviceConfig.config.root.render)

    val secondaries = SecondaryLoader.load(serviceConfig.secondary.configs, serviceConfig.secondary.path)

    for(dsInfo <- DataSourceFromConfig(serviceConfig.dataSource)) {
      val executorService = Executors.newCachedThreadPool()
      try {
        val common = locally {
          new SoQLCommon(
            dsInfo.dataSource,
            dsInfo.copyIn,
            executorService,
            _ => Some("pg_default"),
            new LoggedTimingReport(org.slf4j.LoggerFactory.getLogger("timing-report")) with StackedTimingReport,
            allowDdlOnPublishedCopies = serviceConfig.allowDdlOnPublishedCopies,
            serviceConfig.writeLockTimeout,
            serviceConfig.instance,
            serviceConfig.reports.directory,
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

        val serv = new Service(operations.processMutation, operations.processCreation, getSchema _,
          operations.exporter, secondaries.keySet, operations.datasetsInStore, operations.versionInStore,
          operations.ensureInSecondary, operations.secondariesOfDataset, operations.listDatasets, operations.deleteDataset,
          serviceConfig.commandReadLimit, common.internalNameFromDatasetId, common.datasetIdFromInternalName, operations.makeReportTemporaryFile)

        val finished = new CountDownLatch(1)
        val tableDropper = new Thread() {
          setName("table dropper")
          override def run() {
            while(!finished.await(30, TimeUnit.SECONDS)) {
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
            }
          }
        }

        try {
          tableDropper.start()
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

          log.info("Shutting down secondaries")
          secondaries.values.foreach(_.shutdown())
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
