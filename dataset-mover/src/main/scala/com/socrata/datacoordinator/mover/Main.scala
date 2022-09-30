package com.socrata.datacoordinator.mover

import scala.annotation.tailrec
import scala.concurrent.duration._

import java.io.File
import java.util.concurrent.Executors

import com.rojoma.simplearm.v2._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.PropertyConfigurator
import org.postgresql.PGConnection

import com.socrata.http.client.HttpClientHttpClient
import com.socrata.soql.types.SoQLType
import com.socrata.thirdparty.typesafeconfig.Propertizer

import com.socrata.datacoordinator.common.{DataSourceFromConfig, SoQLCommon}
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.service.{Main => ServiceMain}
import com.socrata.datacoordinator.util.{NoopTimingReport, NullCache}
import com.socrata.datacoordinator.truth.metadata.sql.PostgresDatasetMapWriter
import com.socrata.datacoordinator.truth.metadata.LifecycleStage

sealed abstract class Main

object Main extends App {
  val fromDatasetId = new DatasetId(args(0).toLong)

  val serviceConfig = try {
    new MoverConfig(ConfigFactory.load(), "com.socrata.coordinator.datasetmover", identity)
  } catch {
    case e: Exception =>
      Console.err.println(e)
      sys.exit(1)
  }

  PropertyConfigurator.configure(Propertizer("log4j", serviceConfig.logProperties))

  val log = org.slf4j.LoggerFactory.getLogger(classOf[Main])

  implicit val executorShutdown = Resource.executorShutdownNoTimeout

  for {
    fromDsInfo <- DataSourceFromConfig(serviceConfig.from.dataSource)
    toDsInfo <- DataSourceFromConfig(serviceConfig.to.dataSource)
    executorService <- managed(Executors.newCachedThreadPool)
    httpClient <- managed(new HttpClientHttpClient(executorService))
    tmpDir <- ResourceUtil.Temporary.Directory.managed[File]()
  } {
    val fromCommon = new SoQLCommon(
      fromDsInfo.dataSource,
      fromDsInfo.copyIn,
      executorService,
      ServiceMain.tablespaceFinder(serviceConfig.tablespace),
      NoopTimingReport,
      allowDdlOnPublishedCopies = true,
      serviceConfig.writeLockTimeout,
      serviceConfig.from.instance,
      tmpDir,
      10000.days,
      10000.days,
      NullCache
    )

    val toCommon = new SoQLCommon(
      toDsInfo.dataSource,
      toDsInfo.copyIn,
      executorService,
      ServiceMain.tablespaceFinder(serviceConfig.tablespace),
      NoopTimingReport,
      allowDdlOnPublishedCopies = true,
      serviceConfig.writeLockTimeout,
      serviceConfig.to.instance,
      tmpDir,
      10000.days,
      10000.days,
      NullCache
    )

    for {
      fromLockUniverse <- fromCommon.universe
      fromUniverse <- fromCommon.universe
      toUniverse <- toCommon.universe
    } {
      fromLockUniverse.datasetMapWriter.datasetInfo(fromDatasetId, serviceConfig.writeLockTimeout, true).getOrElse {
        throw new Exception("Can't find dataset")
      }

      val fromMapReader = fromUniverse.datasetMapReader
      val fromDsInfo = fromMapReader.datasetInfo(fromDatasetId).getOrElse {
        throw new Exception("Can't find dataset")
      }

      log.info("Found source dataset {}", fromDsInfo.systemId)

      val toMapWriter = toUniverse.datasetMapWriter.asInstanceOf[PostgresDatasetMapWriter[SoQLType]]
      var toDsInfo = toMapWriter.create(fromDsInfo.localeName, fromDsInfo.resourceName).datasetInfo
      // wow, glad we never killed this code when the backup sytem went away...
      toDsInfo = toMapWriter.unsafeReloadDataset(toDsInfo, fromDsInfo.nextCounterValue, fromDsInfo.latestDataVersion, fromDsInfo.localeName, fromDsInfo.obfuscationKey, fromDsInfo.resourceName)

      log.info("Created target dataset {}", toDsInfo.systemId)

      val fromCopies = fromMapReader.allCopies(fromDsInfo)

      log.info("There are {} copies to move", fromCopies.size)

      val toCopies = fromCopies.iterator.map { fromCopy =>
        log.info("Creating metadata for copy {}", fromCopy.copyNumber)

        val toCopy = toMapWriter.unsafeCreateCopyAllocatingSystemId(
          toDsInfo,
          fromCopy.copyNumber,
          fromCopy.lifecycleStage,
          fromCopy.dataVersion,
          fromCopy.dataShapeVersion,
          fromCopy.lastModified
        )

        val fromColumns = fromMapReader.schema(fromCopy).iterator.map(_._2).toVector.sortBy(_.systemId)
        log.info("There are {} columns to create", fromColumns.size)
        val indexDirectives = fromMapReader.indexDirectives(fromCopy, None).groupBy(_.columnInfo.systemId)
        for(fromColumnInfo <- fromColumns) {
          // this takes care of inserting into computation_strategy_map if relevant
          log.info("Creating column {} ({})", fromColumnInfo.systemId, fromColumnInfo.fieldName)
          val toColumnInfo = toMapWriter.addColumnWithId(fromColumnInfo.systemId, toCopy, fromColumnInfo.userColumnId, fromColumnInfo.fieldName, fromColumnInfo.typ, fromColumnInfo.physicalColumnBaseBase, fromColumnInfo.computationStrategyInfo)
          for(indexDirective <- indexDirectives.getOrElse(toColumnInfo.systemId, Nil)) {
            log.info("Creating index directive")
            toMapWriter.createOrUpdateIndexDirective(toColumnInfo, indexDirective.directive)
          }
        }

        for(rollup <- fromMapReader.rollups(fromCopy)) {
          log.info("Creating rollup {}", rollup.name)
          toMapWriter.createOrUpdateRollup(toCopy, rollup.name, rollup.soql, rollup.rawSoql)
        }
        for(index <- fromMapReader.indexes(fromCopy)) {
          log.info("Creating index {}", index.name)
          toMapWriter.createOrUpdateIndex(toCopy, index.name, index.expressions, index.filter)
        }

        toCopy
      }.toVector

      fromUniverse.rollback() // release any locks on the maps we were holding
      // toUniverse.commit()

      // ok, we've created and populated our maps, now let's copy the data...
      for((fromCopy, toCopy) <- fromCopies.zip(toCopies)) {
        if(fromCopy.lifecycleStage != LifecycleStage.Discarded) {
          // for this, we want to bypass the API as much as possible.
          // We're just going to COPY out of the old table and COPY back
          // into the new table.
          val fromTable = fromCopy.dataTableName
          val toTable = toCopy.dataTableName
          log.info("Copying rows from {} to {}", fromTable:Any, toTable:Any)

          val fromColumns = toUniverse.datasetMapReader.schema(toCopy).iterator.map(_._2).toVector.sortBy(_.systemId)
          val toColumns = toUniverse.datasetMapReader.schema(toCopy).iterator.map(_._2).toVector.sortBy(_.systemId)

          log.info("Creating {}", toTable)
          for(mutationContext <- toUniverse.datasetMutator.databaseMutator.openDatabase) {
            val loader = mutationContext.schemaLoader(new com.socrata.datacoordinator.truth.loader.NullLogger)
            loader.create(toCopy)
            loader.addColumns(toColumns)
          }

          val fromConn = fromUniverse.unsafeRawConnection
          val toConn = toUniverse.unsafeRawConnection

          val fromCopyOutSql = fromColumns.flatMap(fromCommon.sqlRepFor(_).physColumns).mkString(s"COPY $fromTable (", ",", ") TO STDOUT WITH (format csv, header true)")
          val toCopyInSql = toColumns.flatMap(toCommon.sqlRepFor(_).physColumns).mkString(s"COPY $toTable (", ",", ") FROM STDOUT WITH (format csv, header true)")
          val fromCopyOut = fromConn.asInstanceOf[PGConnection].getCopyAPI.copyOut(fromCopyOutSql)
          try {
            val toCopyIn = toConn.asInstanceOf[PGConnection].getCopyAPI.copyIn(toCopyInSql)
            try {
              @tailrec
              def loop(total: Long) {
                val chunk = fromCopyOut.readFromCopy()
                if(chunk ne null) {
                  toCopyIn.writeToCopy(chunk, 0, chunk.length)
                  val newTotal = total + chunk.length
                  print(s"\r$newTotal")
                  System.out.flush()
                  loop(newTotal)
                }
              }
              loop(0)
              toCopyIn.endCopy()
              println()
            } finally {
              if(toCopyIn.isActive) toCopyIn.cancelCopy()
            }
          } finally {
            if(fromCopyOut.isActive) fromCopyOut.cancelCopy()
          }

          for(mutationContext <- toUniverse.datasetMutator.databaseMutator.openDatabase) {
            val loader = mutationContext.schemaLoader(new com.socrata.datacoordinator.truth.loader.NullLogger)
            for(column <- toColumns) {
              if(column.isSystemPrimaryKey) {
                log.info("Making {} the system PK", column.systemId)
                loader.makeSystemPrimaryKey(column)
              }
              if(column.isUserPrimaryKey) {
                log.info("Making {} the user PK", column.systemId)
                loader.makePrimaryKey(column)
              }
            }
          }
        }
      }

      // Ok at this point we've updated the maps and copied the data,
      // now we need to inform the secondaries that the data have
      // moved...

      println("Moved " + fromCommon.internalNameFromDatasetId(fromDsInfo.systemId) + " to " + toCommon.internalNameFromDatasetId(toDsInfo.systemId))

      toUniverse.rollback()
    }
  }
}
