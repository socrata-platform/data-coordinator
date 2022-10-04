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
import com.socrata.datacoordinator.secondary.SecondaryManifest
import com.socrata.datacoordinator.truth.metadata.sql.PostgresDatasetMapWriter
import com.socrata.datacoordinator.truth.metadata.LifecycleStage

sealed abstract class Main

object Main extends App {
  val dryRun = sys.env.get("SOCRATA_COMMIT_MOVE").isEmpty

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

  def fullyReplicated(datasetId: DatasetId, manifest: SecondaryManifest, targetVersion: Long): Boolean = {
    manifest.stores(datasetId).values.forall(_ == targetVersion)
  }

  def isPgSecondary(store: String): Boolean =
    store.startsWith("pg") || store == "read"

  if(serviceConfig.from.dataSource.poolOptions.isDefined) {
    throw new Exception("`from` must not be a c3p0 data source")
  }

  if(serviceConfig.to.dataSource.poolOptions.isDefined) {
    throw new Exception("`to` must not be a c3p0 data source")
  }

  using(new ResourceScope) { rs =>
    val fromDsInfo = DataSourceFromConfig(serviceConfig.from.dataSource, rs)
    val toDsInfo = DataSourceFromConfig(serviceConfig.to.dataSource, rs)
    val executorService = rs.open(Executors.newCachedThreadPool)
    val httpClient = rs.open(new HttpClientHttpClient(executorService))
    val tmpDir = ResourceUtil.Temporary.Directory.scoped[File](rs)

    val sodaFountain = DataSourceFromConfig(serviceConfig.sodaFountain, rs)
    val secondaries = serviceConfig.secondaries.iterator.map { case (k, v) =>
      k -> DataSourceFromConfig(v, rs)
    }.toMap

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
      val fromDsInfo = locally {
        var dsInfo = fromMapReader.datasetInfo(fromDatasetId).getOrElse {
          throw new Exception("Can't find dataset")
        }
        while(!fullyReplicated(dsInfo.systemId, fromUniverse.secondaryManifest, dsInfo.latestDataVersion)) {
          fromUniverse.rollback()
          log.info("zzzzzzz....")
          Thread.sleep(10000)
          dsInfo = fromMapReader.datasetInfo(fromDatasetId).getOrElse {
            throw new Exception("Can't find dataset")
          }
        }
        dsInfo
      }

      val stores = fromUniverse.secondaryManifest.stores(fromDsInfo.systemId).keySet
      if(stores.isEmpty) {
        throw new Exception("Refusing to move dataset that lives in no stores")
      }
      val invalidSecondaries = stores -- serviceConfig.acceptableSecondaries
      if(invalidSecondaries.nonEmpty) {
        throw new Exception("Refusing to move dataset that lives in " + invalidSecondaries)
      }

      val fromInternalName = fromCommon.internalNameFromDatasetId(fromDsInfo.systemId)
      log.info("Found source dataset {}", fromInternalName)

      val toMapWriter = toUniverse.datasetMapWriter.asInstanceOf[PostgresDatasetMapWriter[SoQLType]]
      var toDsInfo = toMapWriter.create(fromDsInfo.localeName, fromDsInfo.resourceName).datasetInfo
      // wow, glad we never killed this code when the backup sytem went away...
      toDsInfo = toMapWriter.unsafeReloadDataset(toDsInfo, fromDsInfo.nextCounterValue, fromDsInfo.latestDataVersion, fromDsInfo.localeName, fromDsInfo.obfuscationKey, fromDsInfo.resourceName)

      val toInternalName = toCommon.internalNameFromDatasetId(toDsInfo.systemId)

      log.info("Created target dataset {}", toInternalName)

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

      // ok, we've created and populated our maps, now let's copy the data...
      for((fromCopy, toCopy) <- fromCopies.zip(toCopies)) {
        if(fromCopy.lifecycleStage != LifecycleStage.Discarded) {
          // for this, we want to bypass the API as much as possible.
          // We're just going to COPY out of the old table and COPY back
          // into the new table.
          val fromTable = fromCopy.dataTableName
          val toTable = toCopy.dataTableName
          log.info("Copying rows from {} to {}", fromTable:Any, toTable:Any)

          val fromColumns = fromUniverse.datasetMapReader.schema(fromCopy).iterator.map(_._2).toVector.sortBy(_.systemId)
          val toColumns = toUniverse.datasetMapReader.schema(toCopy).iterator.map(_._2).toVector.sortBy(_.systemId)

          log.info("Creating {}", toTable)
          for(mutationContext <- toUniverse.datasetMutator.databaseMutator.openDatabase) {
            val loader = mutationContext.schemaLoader(new com.socrata.datacoordinator.truth.loader.NullLogger)
            loader.create(toCopy)
            loader.addColumns(toColumns)
          }

          val fromConn = fromUniverse.unsafeRawConnection
          val toConn = toUniverse.unsafeRawConnection

          val fromCopyOutSql = fromColumns.flatMap(fromCommon.sqlRepFor(_).physColumns).mkString(s"COPY $fromTable (", ",", ") TO STDOUT WITH (format binary)")
          val toCopyInSql = toColumns.flatMap(toCommon.sqlRepFor(_).physColumns).mkString(s"COPY $toTable (", ",", ") FROM STDIN WITH (format binary)")
          val fromCopyOut = fromConn.asInstanceOf[PGConnection].getCopyAPI.copyOut(fromCopyOutSql)
          try {
            val toCopyIn = toConn.asInstanceOf[PGConnection].getCopyAPI.copyIn(toCopyInSql)
            try {
              // This buffering is WAY faster than just writing the chunks we get straight back out
              val buffer = new Array[Byte](1024*1024)
              var bufferEnd = 0

              @tailrec
              def loop(total: Long, lastWrote: Long) {
                val chunk = fromCopyOut.readFromCopy()
                if(chunk ne null) {
                  var written = 0L
                  if(chunk.length + bufferEnd > buffer.length && bufferEnd != 0) {
                    toCopyIn.writeToCopy(buffer, 0, bufferEnd)
                    written += bufferEnd
                    bufferEnd = 0
                  }
                  if(chunk.length > buffer.length) {
                    toCopyIn.writeToCopy(chunk, 0, chunk.length)
                    written += chunk.length
                  } else {
                    System.arraycopy(chunk, 0, buffer, bufferEnd, chunk.length)
                    bufferEnd += chunk.length
                  }
                  val newTotal = total + written
                  val now = System.nanoTime()
                  if(now - lastWrote > 1000000000L) {
                    print(s"\r$newTotal")
                    System.out.flush()
                    loop(newTotal, now)
                  } else {
                    loop(newTotal, lastWrote)
                  }
                } else if(bufferEnd != 0) {
                  toCopyIn.writeToCopy(buffer, 0, bufferEnd)
                  val newTotal = total + bufferEnd
                  bufferEnd = 0
                  println("\r" + newTotal)
                } else {
                  println("\r" + total)
                }
              }
              loop(0, System.nanoTime() - 1000000001L)
              toCopyIn.endCopy()
            } finally {
              if(toCopyIn.isActive) toCopyIn.cancelCopy()
            }
          } finally {
            if(fromCopyOut.isActive) fromCopyOut.cancelCopy()
          }

          for(mutationContext <- toUniverse.datasetMutator.databaseMutator.openDatabase) {
            val loader = mutationContext.schemaLoader(new com.socrata.datacoordinator.truth.loader.NullLogger)
            for((fromColumn, toColumn) <- fromColumns.zip(toColumns)) {
              if(fromColumn.isVersion) {
                log.info("Making {} the version", toColumn.systemId)
                toMapWriter.setVersion(toColumn)
              }
              if(fromColumn.isSystemPrimaryKey) {
                log.info("Making {} the system PK", toColumn.systemId)
                loader.makeSystemPrimaryKey(toColumn)
                toMapWriter.setSystemPrimaryKey(toColumn)
              }
              if(fromColumn.isUserPrimaryKey) {
                log.info("Making {} the user PK", toColumn.systemId)
                loader.makePrimaryKey(toColumn)
                toMapWriter.setUserPrimaryKey(toColumn)
              }
            }
            val newTo = toUniverse.datasetMapReader.schema(toCopy).iterator.map(_._2.unanchored).toVector.sortBy(_.systemId)
            if(newTo != fromColumns.map(_.unanchored)) {
              System.err.println(newTo)
              System.err.println(fromColumns.map(_.unanchored))
              throw new Exception("Schema mismatch post update")
            }
          }
        }
      }

      // Ok at this point we've updated the maps and copied the data,
      // now we need to inform the secondaries that the data have
      // moved...

      // sadness; updating the secondary-manifest is a little more
      // fragile than I'd like.
      log.info("Copying secondary_manifest records to the new truth...")
      for {
        fromStmt <- managed(fromUniverse.unsafeRawConnection.prepareStatement("select store_id, latest_secondary_data_version, latest_data_version, went_out_of_sync_at, cookie, broken_at, broken_acknowledged_at, claimant_id, pending_drop from secondary_manifest where dataset_system_id = ?")).
          and { stmt =>
            stmt.setLong(1, fromDsInfo.systemId.underlying)
          }
        toStmt <- managed(toUniverse.unsafeRawConnection.prepareStatement("insert into secondary_manifest (dataset_system_id, store_id, latest_secondary_data_version, latest_data_version, went_out_of_sync_at, cookie, broken_at, broken_acknowledged_at) values (?, ?, ?, ?, ?, ?, ?, ?)"))
        rs <- managed(fromStmt.executeQuery())
      } {
        while(rs.next()) {
          val storeId = rs.getString(1)
          val latestSecondaryDataVersion = rs.getLong(2)
          val latestDataVersion = rs.getLong(3)
          val wentOutOfSyncAt = rs.getTimestamp(4)
          val cookie = Option(rs.getString(5))
          val brokenAt = Option(rs.getTimestamp(6))
          val brokenAcknowledgedAt = Option(rs.getTimestamp(7))
          val claimantId = Option(rs.getString(8))
          val pendingDrop = rs.getBoolean(9)

          if(!serviceConfig.acceptableSecondaries(storeId)) {
            throw new Exception("Secondary is in an unsupported store!!!  After we checked that it wasn't?!?!?")
          }

          if(claimantId.isDefined) {
            throw new Exception("Secondary manifest record is claimed?")
          }

          if(pendingDrop) {
            throw new Exception("Pending drop?")
          }

          if(latestDataVersion != fromDsInfo.latestDataVersion) {
            throw new Exception("Dataset not actually up to date?")
          }

          if(latestSecondaryDataVersion != fromDsInfo.latestDataVersion) {
            throw new Exception("Dataset not fully replicated after we checked that it was?")
          }

          toStmt.setLong(1, toDsInfo.systemId.underlying)
          toStmt.setString(2, storeId)
          toStmt.setLong(3, latestSecondaryDataVersion)
          toStmt.setLong(4, latestDataVersion)
          toStmt.setTimestamp(5, wentOutOfSyncAt)
          toStmt.setString(6, cookie.orNull)
          toStmt.setTimestamp(7, brokenAt.orNull)
          toStmt.setTimestamp(8, brokenAcknowledgedAt.orNull)

          toStmt.executeUpdate()
        }
      }

      log.info("Adding the new name to the PG secondary stores...")
      for(store <- stores if isPgSecondary(store)) {
        for {
          conn <- managed(secondaries(store).dataSource.getConnection()).
            and(_.setAutoCommit(false))
          stmt <- managed(conn.prepareStatement("insert into dataset_internal_name_map (dataset_internal_name, dataset_system_id, disabled) select ?, dataset_system_id, disabled from dataset_internal_name_map where dataset_internal_name = ?")).
            and { stmt =>
              stmt.setString(1, toInternalName)
              stmt.setString(2, fromInternalName)
            }
        } {
          stmt.executeUpdate()
          if(dryRun) conn.rollback()
          else conn.commit()
        }
      }

      // And now we commit the change into the target truth...
      if(dryRun) toUniverse.rollback()
      else toUniverse.commit()
      fromUniverse.rollback()

      log.info("Informing soda-fountain of the internal name change...")
      for {
        conn <- managed(sodaFountain.dataSource.getConnection()).
          and(_.setAutoCommit(false))
        datasetStmt <- managed(conn.prepareStatement("update datasets set dataset_system_id = ? where dataset_system_id = ?")).
          and { stmt =>
            stmt.setString(1, toInternalName)
            stmt.setString(2, fromInternalName)
          }
        // Turns out dataset_copies doesn't have a FK on datasets.  Lucky us.
        copyStmt <- managed(conn.prepareStatement("update dataset_copies set dataset_system_id = ? where dataset_system_id = ?")).
          and { stmt =>
            stmt.setString(1, toInternalName)
            stmt.setString(2, fromInternalName)
          }
      } {
        datasetStmt.executeUpdate()
        copyStmt.executeUpdate()
        if(dryRun) conn.rollback()
        else conn.commit()
      }

      log.info("Pausing to let everything work through...")
      Thread.sleep(6000)

      log.info("Remvoing secondary_manifest records on the old store...")
      for(store <- stores) {
        fromUniverse.secondaryMetrics.dropDataset(store, fromDsInfo.systemId)
        fromUniverse.secondaryManifest.dropDataset(store, fromDsInfo.systemId)
      }

      log.info("Removing the ex-name from pg secondaries...")
      for(store <- stores if isPgSecondary(store)) {
        for {
          conn <- managed(secondaries(store).dataSource.getConnection()).
            and(_.setAutoCommit(false))
          stmt <- managed(conn.prepareStatement("delete from dataset_internal_name_map where dataset_internal_name = ?")).
            and { stmt =>
              stmt.setString(1, fromInternalName)
            }
        } {
          stmt.executeUpdate()
          if(dryRun) conn.rollback()
          else conn.commit()
        }
      }

      if(dryRun) {
        toUniverse.rollback()
        fromUniverse.rollback()
      }

      println("Moved " + fromInternalName + " to " + toInternalName)
    }
  }
}
