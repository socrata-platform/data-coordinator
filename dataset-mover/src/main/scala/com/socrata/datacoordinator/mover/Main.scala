package com.socrata.datacoordinator.mover

import scala.annotation.tailrec
import scala.concurrent.duration._

import java.io.File
import java.net.URL
import java.sql.Connection
import java.util.concurrent.Executors

import com.rojoma.simplearm.v2._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.PropertyConfigurator
import org.postgresql.PGConnection
import org.postgresql.copy.{CopyIn,CopyOut}

import com.socrata.http.client.HttpClientHttpClient
import com.socrata.soql.types.SoQLType
import com.socrata.thirdparty.typesafeconfig.Propertizer

import com.socrata.datacoordinator.common.{DataSourceFromConfig, SoQLCommon}
import com.socrata.datacoordinator.id.{DatasetId, DatasetInternalName}
import com.socrata.datacoordinator.service.{Main => ServiceMain}
import com.socrata.datacoordinator.util.{NoopTimingReport, NullCache}
import com.socrata.datacoordinator.secondary.SecondaryManifest
import com.socrata.datacoordinator.truth.metadata.sql.PostgresDatasetMapWriter
import com.socrata.datacoordinator.truth.metadata.LifecycleStage

sealed abstract class Main

object Main extends App {
  if(args.length != 2) {
    System.err.println("Usage: dataset-mover.jar INTERNAL_NAME TARGET_TRUTH")
    System.err.println()
    System.err.println("  INTERNAL_NAME   internal name (e.g., alpha.1234) of the dataset to move")
    System.err.println("  TARGET_TRUTH    truthstore in which to move it (e.g., bravo)")
    System.err.println()
    System.err.println("Unless the SOCRATA_COMMIT_MOVE environment variable is set, all changes")
    System.err.println("will be rolled back rather than committed.")
    sys.exit(1)
  }

  val fromInternalName = DatasetInternalName(args(0)).getOrElse {
    System.err.println("Illegal dataset internal name")
    sys.exit(1)
  }
  val fromDatasetId = fromInternalName.datasetId

  val toInstance = args(1)

  val dryRun = sys.env.get("SOCRATA_COMMIT_MOVE").isEmpty

  val serviceConfig = try {
    new MoverConfig(ConfigFactory.load(), "com.socrata.coordinator.datasetmover")
  } catch {
    case e: Exception =>
      Console.err.println(e)
      sys.exit(1)
  }

  PropertyConfigurator.configure(Propertizer("log4j", serviceConfig.logProperties))

  val log = org.slf4j.LoggerFactory.getLogger(classOf[Main])

  implicit val executorShutdown = Resource.executorShutdownNoTimeout

  def fullyReplicated(datasetId: DatasetId, manifest: SecondaryManifest, targetVersion: Long): Boolean = {
    manifest.stores(datasetId).values.forall{ case (version: Long, _) => version >= targetVersion }
  }

  def isPgSecondary(store: String): Boolean =
    serviceConfig.pgSecondaries.contains(store)

  if(serviceConfig.truths.values.exists(_.poolOptions.isDefined)) {
    System.err.println("truths must not be c3p0 data sources")
    sys.exit(1)
  }

  def doCopy(fromConn: Connection, copyOutSql: String, toConn: Connection, copyInSql: String) {
    log.info("Copy from: {}", copyOutSql)
    log.info("Copy to: {}", copyInSql)
    val fromCopyOut = fromConn.asInstanceOf[PGConnection].getCopyAPI.copyOut(copyOutSql)
    try {
      val toCopyIn = toConn.asInstanceOf[PGConnection].getCopyAPI.copyIn(copyInSql)
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
  }

  val acceptableSecondaries =
    serviceConfig.pgSecondaries.keySet ++
      serviceConfig.archivalUrl.map(_ => "archival") ++
      serviceConfig.additionalAcceptableSecondaries

  class Bail(msg: String) extends Exception(msg)
  def bail(msg: String): Nothing = throw new Bail(msg)

  try {
    using(new ResourceScope) { rs =>
      val executorService = rs.open(Executors.newCachedThreadPool)
      val httpClient = rs.open(new HttpClientHttpClient(executorService))
      val tmpDir = ResourceUtil.Temporary.Directory.scoped[File](rs)

      val truths = serviceConfig.truths.iterator.map { case (k, v) =>
        k -> DataSourceFromConfig(v, rs)
      }.toMap
      val secondaries = serviceConfig.pgSecondaries.iterator.map { case (k, v) =>
        k -> DataSourceFromConfig(v, rs)
      }.toMap
      val sodaFountain = DataSourceFromConfig(serviceConfig.sodaFountain, rs)

      val archivalSecondaryClient = serviceConfig.archivalUrl.map { url =>
        new ArchivalSecondaryClient(httpClient, new URL(url))
      }
      archivalSecondaryClient.foreach(_.check())

      val fromDsInfo = truths(fromInternalName.instance)
      val toDsInfo = truths(toInstance)

      val commons = truths.iterator.map { case (instance, dsInfo) =>
        instance -> new SoQLCommon(
          dsInfo.dataSource,
          dsInfo.copyIn,
          executorService,
          ServiceMain.tablespaceFinder(serviceConfig.tablespace),
          NoopTimingReport,
          allowDdlOnPublishedCopies = true,
          serviceConfig.writeLockTimeout,
          instance,
          tmpDir,
          10000.days,
          10000.days,
          NullCache
        )
      }.toMap

      val fromCommon = commons(fromInternalName.instance)
      val toCommon = commons(toInstance)

      val otherTruthUniverses = (commons -- Seq(fromInternalName.instance, toInstance)).iterator.map { case (instance, common) =>
        instance -> common.scopedUniverse(rs)
      }.toMap

      for {
        fromLockUniverse <- fromCommon.universe
        fromUniverse <- fromCommon.universe
        toUniverse <- toCommon.universe
      } {
        fromLockUniverse.datasetMapWriter.datasetInfo(fromDatasetId, serviceConfig.writeLockTimeout, false).getOrElse {
          bail("Can't find dataset")
        }

        val fromMapReader = fromUniverse.datasetMapReader
        val fromDsInfo = locally {
          var dsInfo = fromMapReader.datasetInfo(fromDatasetId).getOrElse {
            bail("Can't find dataset")
          }
          while(!fullyReplicated(dsInfo.systemId, fromUniverse.secondaryManifest, dsInfo.latestDataVersion)) {
            fromUniverse.rollback()
            log.info("zzzzzzz....")
            Thread.sleep(10000)
            dsInfo = fromMapReader.datasetInfo(fromDatasetId).getOrElse {
              bail("Can't find dataset")
            }
          }
          dsInfo
        }

        val stores = fromUniverse.secondaryManifest.stores(fromDsInfo.systemId).keySet
        if(stores.isEmpty) {
          bail("Refusing to move dataset that lives in no stores")
        }
        val invalidSecondaries = stores -- acceptableSecondaries
        if(invalidSecondaries.nonEmpty) {
          bail("Refusing to move dataset that lives in " + invalidSecondaries)
        }

        log.info("Found source dataset {}", fromInternalName)

        val toMapWriter = toUniverse.datasetMapWriter.asInstanceOf[PostgresDatasetMapWriter[SoQLType]]
        var toDsInfo = toMapWriter.create(fromDsInfo.localeName, fromDsInfo.resourceName).datasetInfo
        // wow, glad we never killed this code when the backup sytem went away...
        toDsInfo = toMapWriter.unsafeReloadDataset(toDsInfo, fromDsInfo.nextCounterValue, fromDsInfo.latestDataVersion, fromDsInfo.localeName, fromDsInfo.obfuscationKey, fromDsInfo.resourceName)

        val toInternalName = toCommon.datasetInternalNameFromDatasetId(toDsInfo.systemId)

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

            doCopy(
              fromConn,
              fromColumns.flatMap(fromCommon.sqlRepFor(_).physColumns).mkString(s"COPY $fromTable (", ",", ") TO STDOUT WITH (format binary)"),
              toConn,
              toColumns.flatMap(toCommon.sqlRepFor(_).physColumns).mkString(s"COPY $toTable (", ",", ") FROM STDIN WITH (format binary)")
            )

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
                bail("Schema mismatch post update")
              }
            }
          }
        }

        doCopy(
          fromUniverse.unsafeRawConnection,
          s"COPY ${fromDsInfo.auditTableName} (version, who, at_time) TO STDOUT WITH (format binary)",
          toUniverse.unsafeRawConnection,
          s"COPY ${toDsInfo.auditTableName} (version, who, at_time) FROM STDIN WITH (format binary)"
        )

        doCopy(
          fromUniverse.unsafeRawConnection,
          s"COPY ${fromDsInfo.logTableName} (version, subversion, what, aux) TO STDOUT WITH (format binary)",
          toUniverse.unsafeRawConnection,
          s"COPY ${toDsInfo.logTableName} (version, subversion, what, aux) FROM STDIN WITH (format binary)"
        )

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

            if(!acceptableSecondaries(storeId)) {
              bail("Secondary is in an unsupported store!!!  After we checked that it wasn't?!?!?")
            }

            if(claimantId.isDefined) {
              bail("Secondary manifest record is claimed?")
            }

            if(pendingDrop) {
              bail("Pending drop?")
            }

            if(latestDataVersion != fromDsInfo.latestDataVersion) {
              bail("Dataset not actually up to date?")
            }

            if(latestSecondaryDataVersion < fromDsInfo.latestDataVersion) {
              bail("Dataset not fully replicated after we checked that it was?")
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

        log.info("Copying collocation_manifest records to the new truth...")
        // Any secondary_manifest records held in _this_ truth need to
        // be put in the other truth and have their reference changed
        // from the old internal name to the new internal name.
        for {
          fromStmt <- managed(fromUniverse.unsafeRawConnection.prepareStatement("select dataset_internal_name_left, dataset_internal_name_right, created_at, updated_at from collocation_manifest where dataset_internal_name_left = ? or dataset_internal_name_right = ?"))
            .and { stmt =>
              stmt.setString(1, fromInternalName.underlying)
              stmt.setString(2, fromInternalName.underlying)
            }
          fromResults <- managed(fromStmt.executeQuery())
          toStmt <- managed(toUniverse.unsafeRawConnection.prepareStatement("insert into collocation_manifest(dataset_internal_name_left, dataset_internal_name_right, created_at, updated_at) values (?, ?, ?, ?)"))
        } {
          def switchName(s: String) =
            if(s == fromInternalName.underlying) toInternalName.underlying else s

          while(fromResults.next()) {
            log.info(" - {}/{} => {}/{}", fromResults.getString(1), fromResults.getString(2), switchName(fromResults.getString(1)), switchName(fromResults.getString(2)))
            toStmt.setString(1, switchName(fromResults.getString(1)))
            toStmt.setString(2, switchName(fromResults.getString(2)))
            toStmt.setTimestamp(3, fromResults.getTimestamp(3))
            toStmt.setTimestamp(4, fromResults.getTimestamp(4))
            toStmt.executeUpdate()
          }
        }
        // and for all other truths, we want to simply add a new
        // record to this truth's manifest table to say "if you were
        // collocated with old-dataset before, now you're also
        // collocated with new-dataset"
        for((truth, universe) <- otherTruthUniverses ++ Map(toInternalName.instance -> toUniverse)) {
          val count1 =
            managed(universe.unsafeRawConnection.prepareStatement("insert into collocation_manifest (dataset_internal_name_left, dataset_internal_name_right, created_at, updated_at) select ?, dataset_internal_name_right, created_at, updated_at from collocation_manifest where dataset_internal_name_left = ?"))
              .and { stmt =>
                stmt.setString(1, toInternalName.underlying)
                stmt.setString(2, fromInternalName.underlying)
              }
              .run(_.executeUpdate())
          log.info(" - Created {} records on {} where the old name was left", count1, truth)

          val count2 =
            managed(universe.unsafeRawConnection.prepareStatement("insert into collocation_manifest (dataset_internal_name_left, dataset_internal_name_right, created_at, updated_at) select dataset_internal_name_left, ?, created_at, updated_at from collocation_manifest where dataset_internal_name_right = ?"))
              .and { stmt =>
                stmt.setString(1, toInternalName.underlying)
                stmt.setString(2, fromInternalName.underlying)
              }
              .run(_.executeUpdate())
          log.info(" - Created {} records on {} where the old name was right", count2, truth)
        }

        log.info("Adding the new name to the PG secondary stores...")
        for(store <- stores if isPgSecondary(store)) {
          for {
            conn <- managed(secondaries(store).dataSource.getConnection()).
              and(_.setAutoCommit(false))
            stmt <- managed(conn.prepareStatement("insert into dataset_internal_name_map (dataset_internal_name, dataset_system_id, disabled) select ?, dataset_system_id, disabled from dataset_internal_name_map where dataset_internal_name = ?")).
              and { stmt =>
                stmt.setString(1, toInternalName.underlying)
                stmt.setString(2, fromInternalName.underlying)
              }
          } {
            stmt.executeUpdate()
            if(dryRun) conn.rollback()
            else conn.commit()
          }
        }

        // Current state of the world:
        //   * PG secondaries know about the dataset under its old AND
        //     new names
        //   * The old truth and the new truth have copies of the
        //     dataset (the new one is uncommitted)
        //   * Both truths have secondary_manifest records
        //   * Archival secondary and soda-fountain know only the old
        //     name
        //   * The dataset is locked against writes

        // First, _before_ committing to the new truth, update the
        // archival secondary.  This is because when we commit the
        // truth change, the archival secondary will see its
        // secondary_manifest record and try to create the dataset, so
        // we need to tell it exists before that happens.
        for(asc <- archivalSecondaryClient if stores.contains("archival")) {
          log.info("Informing archival secondary of the internal name change...")
          if(dryRun) {
            log.info("(not really)")
          } else {
            // If this fails, then we're in a world where the old
            // dataset still exists in the original truth and is still
            // accessible under that name in the PG secondaries.
            // There'll be some manual cleanup to do (namely, undoing
            // the changes to the secondaries' dataset internal name
            // maps and dropping the new copy in truth) but that's
            // fiddly enough that I want a human to do it.
            asc.addName(fromInternalName, toInternalName)
          }
        }

        def rollbackPGSecondary(): Unit = {
          log.info("Rolling back PG secondaries due to exception updating soda-fountain");
          try {
            for(store <- stores if isPgSecondary(store)) {
              for {
                conn <- managed(secondaries(store).dataSource.getConnection()).
                  and(_.setAutoCommit(false))
                stmt <- managed(conn.prepareStatement("delete from dataset_internal_name_map where dataset_internal_name = ?")).
                  and { stmt =>
                    stmt.setString(1, toInternalName.underlying)
                  }
              } {
                stmt.executeUpdate()
                if(dryRun) conn.rollback()
                else conn.commit()
              }
            }
          } catch {
            case e2: Exception =>
              log.error("Failed to roll back PG secondary change", e2)
          }
        }

        def rollbackArchivalSecondary(): Unit = {
          for(asc <- archivalSecondaryClient if stores.contains("archival")) {
            log.info("Soda-fountain update failed; undoing the archival secondary rename")
            try {
              if(dryRun) {
                log.info("(not really)")
              } else {
                // Now, if _this_ fails, we're in a genuinely weird
                // state!
                asc.removeName(fromInternalName, toInternalName)
              }
            } catch {
              case e2: Exception =>
                log.error("Failed to roll back archival secondary change", e2)
            }
          }
        }

        // And now we commit the change into the target truth...
        log.info("Committing target truth")
        try {
          if(dryRun) toUniverse.rollback()
          else toUniverse.commit()
        } catch {
          case e: Exception =>
            rollbackPGSecondary()
            rollbackArchivalSecondary()
            throw e
        }

        fromUniverse.rollback()

        log.info("Committing collocation_manifest changes to other truths")
        // Same comment as a above re: what happens if this fails
        try {
          for((instance, universe) <- otherTruthUniverses) {
            log.info("..{}", instance)
            if(dryRun) universe.rollback()
            else universe.commit()
          }
        } catch {
          case e: Exception =>
            rollbackPGSecondary()
            rollbackArchivalSecondary()
            throw e
        }

        log.info("Informing soda-fountain of the internal name change...")
        // Same comment as a above re: what happens if this fails
        try {
          for {
            conn <- managed(sodaFountain.dataSource.getConnection()).
              and(_.setAutoCommit(false))
            datasetStmt <- managed(conn.prepareStatement("update datasets set dataset_system_id = ? where dataset_system_id = ?")).
              and { stmt =>
                stmt.setString(1, toInternalName.underlying)
                stmt.setString(2, fromInternalName.underlying)
              }
            // Turns out dataset_copies doesn't have a FK on datasets.  Lucky us.
            copyStmt <- managed(conn.prepareStatement("update dataset_copies set dataset_system_id = ? where dataset_system_id = ?")).
              and { stmt =>
                stmt.setString(1, toInternalName.underlying)
                stmt.setString(2, fromInternalName.underlying)
              }
          } {
            datasetStmt.executeUpdate()
            copyStmt.executeUpdate()
            if(dryRun) conn.rollback()
            else conn.commit()
          }
        } catch {
          case e: Exception =>
            // Current state of the world:
            //  * Updating soda-fountain has failed, so it's still
            //    pointing at the old name
            //  * The pg and archive secondaries know about
            //    both the old and the new copies.
            //
            // So we'll try to roll those back.  If this fails, we'll
            // be be in an _especially_ weird state, and will likely
            // require human intervension to fix.
            rollbackPGSecondary()
            rollbackArchivalSecondary()
            throw e
        }

        log.info("Pausing {} to let everything work through...", serviceConfig.postSodaFountainUpdatePause)
        Thread.sleep(serviceConfig.postSodaFountainUpdatePause.toMillis)

        // Current state of the world:
        //   * PG secondaries know about the dataset under its old AND new names
        //   * The old truth and the new truth have copies of the dataset
        //   * Archival secondary and soda-fountain know only the NEW name
        //   * The dataset is locked against writes

        log.info("Removing secondary_manifest records on the old store...")
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
                stmt.setString(1, fromInternalName.underlying)
              }
          } {
            stmt.executeUpdate()
            if(dryRun) conn.rollback()
            else conn.commit()
          }
        }

        log.info("Removing the ex-name from archival secondary...")
        for(asc <- archivalSecondaryClient if stores.contains("archival")) {
          asc.removeName(toInternalName, fromInternalName)
        }

        if(dryRun) {
          toUniverse.rollback()
          fromUniverse.rollback()
        } else {
          toUniverse.commit();
          fromUniverse.commit()
        }

        log.info("Deleting ex-dataset")
        fromLockUniverse.datasetDropper.dropDataset(fromDatasetId) // need to do this on the connection where we have the dataset_map lock
        if(dryRun) {
          fromLockUniverse.rollback()
        } else {
          fromLockUniverse.commit()
        }

        println("Moved " + fromInternalName + " to " + toInternalName)
      }
    }
  } catch {
    case e: Bail =>
      System.err.println(e.getMessage)
      sys.exit(1)
  }
}
