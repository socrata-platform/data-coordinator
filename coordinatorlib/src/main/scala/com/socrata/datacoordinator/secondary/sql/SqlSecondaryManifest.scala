package com.socrata.datacoordinator.secondary
package sql

import java.sql.{Types, Connection, SQLException}
import java.util.UUID

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.id.sql._
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import scala.collection.immutable.VectorBuilder
import com.socrata.datacoordinator.truth.metadata
import com.socrata.datacoordinator.util.PostgresUniqueViolation
import scala.concurrent.duration.FiniteDuration

class SqlSecondaryManifest(conn: Connection) extends SecondaryManifest {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[SqlSecondaryManifest])

  def readLastDatasetInfo(storeId: String, datasetId: DatasetId): Option[(Long, Option[String])] =
    using(conn.prepareStatement("SELECT latest_secondary_data_version, cookie FROM secondary_manifest WHERE store_id = ? AND dataset_system_id = ?")) { stmt =>
      stmt.setString(1, storeId)
      stmt.setDatasetId(2, datasetId)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          Some((rs.getLong("latest_secondary_data_version"), Option(rs.getString("cookie"))))
        } else {
          None
        }
      }
    }

  def addDataset(storeId: String, datasetId: DatasetId): Unit = {
    try {
      using(conn.prepareStatement(
        """INSERT INTO secondary_manifest (store_id, dataset_system_id, latest_data_version)
          | SELECT ?, dataset_system_id, data_version
          |   FROM copy_map
          |   WHERE dataset_system_id = ?
          |   ORDER BY data_version DESC
          |   LIMIT 1""".stripMargin)) { stmt =>
        stmt.setString(1, storeId)
        stmt.setDatasetId(2, datasetId)
        stmt.execute()
        (0L, None)
      }
    } catch {
      case PostgresUniqueViolation(_*) =>
        throw new DatasetAlreadyInSecondary(storeId, datasetId)
    }
  }

  def dropDataset(storeId: String, datasetId: DatasetId): Unit = {
    using(conn.prepareStatement("DELETE FROM secondary_manifest WHERE store_id = ? AND dataset_system_id = ?")) { stmt =>
      stmt.setString(1, storeId)
      stmt.setDatasetId(2, datasetId)
      stmt.execute()
    }
  }

  def statusOf(storeId: String, datasetId: DatasetId): Map[String, Long] = {
    using(conn.prepareStatement("SELECT store_id, latest_secondary_data_version FROM secondary_manifest WHERE dataset_system_id = ?")) { stmt =>
      stmt.setDatasetId(1, datasetId)
      using(stmt.executeQuery()) { rs =>
        val result = Map.newBuilder[String, Long]
        while(rs.next()) {
          result += rs.getString("store_id") -> rs.getLong("latest_secondary_data_version")
        }
        result.result()
      }
    }
  }

  def datasets(storeId: String): Map[DatasetId, Long] = {
    using(conn.prepareStatement("SELECT dataset_system_id, latest_secondary_data_version FROM secondary_manifest WHERE store_id = ?")) { stmt =>
      stmt.setString(1, storeId)
      using(stmt.executeQuery()) { rs =>
        val result = Map.newBuilder[DatasetId, Long]
        while(rs.next()) {
          result += rs.getDatasetId("dataset_system_id") -> rs.getLong("latest_secondary_data_version")
        }
        result.result()
      }
    }
  }

  def stores(datasetId: DatasetId): Map[String, Long] = {
    using(conn.prepareStatement("SELECT store_id, latest_secondary_data_version FROM secondary_manifest WHERE dataset_system_id = ?")) { stmt =>
      stmt.setDatasetId(1, datasetId)
      using(stmt.executeQuery()) { rs =>
        val result = Map.newBuilder[String, Long]
        while(rs.next()) {
          result += rs.getString("store_id") -> rs.getLong("latest_secondary_data_version")
        }
        result.result()
      }
    }
  }

  def claimDatasetNeedingReplication(storeId: String, claimantId: UUID, claimTimeout: FiniteDuration):
      Option[SecondaryRecord] = {
    // EN-5687 and http://dba.stackexchange.com/questions/83171/how-many-rows-will-be-locked-by-select-order-by-xxx-limit-1-for-update
    // the two WHERE clauses below should be the same (wtb named params so we don't have to set them twice)
    // also if we ever get to where everything is on 9.5, we may be able to use
    // https://wiki.postgresql.org/wiki/What's_new_in_PostgreSQL_9.5#SKIP_LOCKED instead
    // but in any case:
    // we're doing the subquery so that we only potentially lock one row instead of hundreds/thousands
    // and we re-assert the filters because things may have changed between the inner and outer queries
    val job = using(conn.prepareStatement(
      """SELECT dataset_system_id
        |  ,latest_secondary_data_version
        |  ,latest_data_version
        |  ,retry_num
        |  ,replay_num
        |  ,cookie
        |  ,pending_drop
        |FROM secondary_manifest
        |WHERE store_id = ?
        |  AND dataset_system_id = (SELECT dataset_system_id
        |                           FROM secondary_manifest
        |                           WHERE store_id = ?
        |                             AND broken_at IS NULL
        |                             AND next_retry <= now()
        |                             AND (latest_data_version > latest_secondary_data_version
        |                               OR pending_drop = TRUE)
        |                             AND (claimant_id is NULL
        |                               OR claimed_at < (CURRENT_TIMESTAMP - CAST (? AS INTERVAL)))
        |                           ORDER BY went_out_of_sync_at
        |                           LIMIT 1)
        |  AND broken_at IS NULL
        |  AND next_retry <= now()
        |  AND (latest_data_version > latest_secondary_data_version
        |    OR pending_drop = TRUE)
        |  AND (claimant_id is NULL
        |    OR claimed_at < (CURRENT_TIMESTAMP - CAST (? AS INTERVAL)))
        |FOR UPDATE""".stripMargin)) { stmt =>
      val claimTimeoutMillisStr = claimTimeout.toMillis + " milliseconds"
      stmt.setString(1, storeId)
      stmt.setString(2, storeId)
      stmt.setString(3, claimTimeoutMillisStr)
      stmt.setString(4, claimTimeoutMillisStr)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          val j = SecondaryRecord(
            storeId,
            claimantId,
            rs.getDatasetId("dataset_system_id"),
            startingDataVersion = rs.getLong("latest_secondary_data_version") + 1,
            endingDataVersion = rs.getLong("latest_data_version"),
            retryNum = rs.getInt("retry_num"),
            replayNum = rs.getInt("replay_num"),
            initialCookie = Option(rs.getString("cookie")),
            pendingDrop = rs.getBoolean("pending_drop"))
          markDatasetClaimedForReplication(j)
          Some(j)
        }
        else None
      }
    }
    conn.commit()
    job
  }

  def cleanOrphanedClaimedDatasets(storeId: String, claimantId: UUID): Unit = {
    using(conn.prepareStatement(
      """SELECT  dataset_system_id
        |  ,latest_secondary_data_version
        |  ,latest_data_version
        |  ,retry_num
        |  ,replay_num
        |  ,cookie
        |  ,pending_drop
        |FROM secondary_manifest
        |WHERE claimant_id = ?
        |  AND store_id = ?""".stripMargin)) {stmt =>
      stmt.setObject(1, claimantId)
      stmt.setString(2, storeId)
      using(stmt.executeQuery()) { rs =>
        while(rs.next()){
          val j = SecondaryRecord(
            storeId,
            claimantId,
            rs.getDatasetId("dataset_system_id"),
            startingDataVersion = rs.getLong("latest_secondary_data_version") + 1,
            endingDataVersion = rs.getLong("latest_data_version"),
            retryNum = rs.getInt("retry_num"),
            replayNum = rs.getInt("replay_num"),
            initialCookie = Option(rs.getString("cookie")),
            pendingDrop = rs.getBoolean("pending_drop"))
          releaseClaimedDataset(j)
        }
      }
    }
  }

  // NOTE: claimed_at is updated in SecondaryWatcherClaimManager.  initially_claimed_at is not.
  def markDatasetClaimedForReplication(job: SecondaryRecord): Unit = {
    using(conn.prepareStatement(
      """UPDATE secondary_manifest
        |SET claimed_at = CURRENT_TIMESTAMP
        |  ,initially_claimed_at = CURRENT_TIMESTAMP
        |  ,claimant_id = ?
        |WHERE store_id = ?
        |  AND dataset_system_id = ?""".stripMargin)) { stmt =>
      stmt.setObject(1, job.claimantId)
      stmt.setString(2, job.storeId)
      stmt.setLong(3, job.datasetId.underlying)
      stmt.executeUpdate()
    }
  }

  def releaseClaimedDataset(job: SecondaryRecord): Unit = {
    val savepoint = conn.setSavepoint()

    // retrying with rollback to savepoint
    def retrying(backoffMillis: Int = 5): Unit = {
      if (backoffMillis > 300000) { // > 5 minutes
        log.error("Ran out of retries; failed to release claim on dataset {} in secondary {}!",
          job.datasetId, job.storeId)
        throw new Exception("Ran out of retries; Failed to release claim on dataset!")
      }

      try {
        log.trace("Attempting to release claim on dataset for job: {}.", job)
        releaseClaimedDatasetUnsafe(job)
      } catch {
        case e: SQLException =>
          log.warn("Unexpected sql exception while releasing claim on dataset {} in secondary {}",
            job.datasetId.asInstanceOf[AnyRef], job.storeId, e)
          conn.rollback(savepoint)
          Thread.sleep(backoffMillis)
          retrying(2 * backoffMillis)
      } finally {
        try {
          conn.releaseSavepoint(savepoint)
        } catch {
          case e: SQLException =>
            // Ignore; this means one of two things:
            // * the server is in an unexpected "transaction aborted" state, so all we
            //    can do is roll back (either to another, earlier savepoint or completely)
            //    and either way this savepoint will be dropped implicitly
            // * things have completely exploded and nothing can be done except
            //    dropping the connection altogether.
            // The latter could happen if this finally block is being run because
            // this method is exiting normally, but in that case whatever we do next
            // will fail so meh.  Just log it and continue.
            log.warn("Unexpected exception releasing savepoint", e)
        }
      }
    }

    retrying()
  }

  private def releaseClaimedDatasetUnsafe(job: SecondaryRecord): Unit = {
    using(conn.prepareStatement(
      """UPDATE secondary_manifest
        |SET claimed_at = NULL
        |  ,claimant_id = NULL
        |WHERE claimant_id = ?
        |  AND store_id = ?
        |  AND dataset_system_id = ?""".stripMargin)) { stmt =>
      stmt.setObject(1, job.claimantId)
      stmt.setString(2, job.storeId)
      stmt.setLong(3, job.datasetId.underlying)
      stmt.executeUpdate()
    }
  }

  def markSecondaryDatasetBroken(job: SecondaryRecord): Unit = {
    using(conn.prepareStatement(
      """UPDATE secondary_manifest
        |SET broken_at = CURRENT_TIMESTAMP
        |WHERE store_id = ?
        |  AND dataset_system_id = ?""".stripMargin)) { stmt =>
      stmt.setString(1, job.storeId)
      stmt.setLong(2, job.datasetId.underlying)
      stmt.executeUpdate()
    }
  }

  def completedReplicationTo(storeId: String,
                             claimantId: UUID,
                             datasetId: DatasetId,
                             dataVersion: Long,
                             cookie: Option[String]): Unit = {
    using(conn.prepareStatement(
      """UPDATE secondary_manifest
        |SET latest_secondary_data_version = ?
        |  ,cookie = ?
        |  ,went_out_of_sync_at = CURRENT_TIMESTAMP
        |WHERE claimant_id = ?
        |  AND store_id = ?
        |  AND dataset_system_id = ?""".stripMargin)) { stmt =>
      stmt.setLong(1, dataVersion)
      cookie match {
        case Some(c) => stmt.setString(2, c)
        case None => stmt.setNull(2, Types.VARCHAR)
      }
      stmt.setObject(3, claimantId)
      stmt.setString(4, storeId)
      stmt.setDatasetId(5, datasetId)
      stmt.executeUpdate()
    }
  }

  def updateRetryInfo(storeId: String, datasetId: DatasetId, retryNum: Int, nextRetryDelaySecs: Int): Unit = {
    using(conn.prepareStatement(
      """UPDATE secondary_manifest
        |SET retry_num = ?
        |  ,next_retry = CURRENT_TIMESTAMP + (? :: INTERVAL)
        |WHERE store_id = ?
        |  AND dataset_system_id = ?""".stripMargin)) { stmt =>
      stmt.setInt(1, retryNum)
      stmt.setString(2, "%s seconds".format(nextRetryDelaySecs))
      stmt.setString(3, storeId)
      stmt.setDatasetId(4, datasetId)
      stmt.executeUpdate()
    }
  }

  def updateReplayInfo(storeId: String, datasetId: DatasetId, cookie: Cookie, replayNum: Int, nextReplayDelaySecs: Int): Unit = {
    using(conn.prepareStatement(
      """UPDATE secondary_manifest
        |SET cookie = ?
        |  ,retry_num = ?
        |  ,replay_num = ?
        |  ,next_retry = CURRENT_TIMESTAMP + (? :: INTERVAL)
        |WHERE store_id = ?
        |  AND dataset_system_id = ?""".stripMargin)) { stmt =>
      cookie match {
        case Some(c) => stmt.setString(1, c)
        case None => stmt.setNull(1, Types.VARCHAR)
      }
      stmt.setInt(2, 0) // start with fresh retry limit
      stmt.setInt(3, replayNum)
      stmt.setString(4, "%s seconds".format(nextReplayDelaySecs))
      stmt.setString(5, storeId)
      stmt.setDatasetId(6, datasetId)
      stmt.executeUpdate()
    }
  }

  def markDatasetForDrop(storeId: String, datasetId: DatasetId): Boolean = {
    using(conn.prepareStatement(
    """UPDATE secondary_manifest
      |SET pending_drop = TRUE
      |WHERE store_id = ?
      |  AND dataset_system_id = ?""".stripMargin)) { stmt =>
      stmt.setString(1, storeId)
      stmt.setDatasetId(2, datasetId)
      stmt.executeUpdate() != 0
    }
  }

  def feedbackSecondaries(datasetId: DatasetId): Set[String] = { // store IDs =
    using(conn.prepareStatement(
      """SELECT sm.store_id
        |  FROM secondary_manifest sm JOIN secondary_stores_config ssc ON sm.store_id = ssc.store_id
        |  WHERE sm.dataset_system_id = ?
        |        AND ssc.is_feedback_secondary""".stripMargin)) { stmt =>
      stmt.setDatasetId(1, datasetId)
      using(stmt.executeQuery()) { rs =>
        val result = Set.newBuilder[String]
        while(rs.next()) result += rs.getString(1)
        result.result()
      }
    }
  }

  def outOfDateFeedbackSecondaries(datasetId: DatasetId): Set[String] = { // store IDs =
    using(conn.prepareStatement(
      """SELECT sm.store_id
        |  FROM secondary_manifest sm JOIN secondary_stores_config ssc ON sm.store_id = ssc.store_id
        |  WHERE sm.dataset_system_id = ?
        |        AND ssc.is_feedback_secondary
        |        AND sm.latest_data_version <> sm.latest_secondary_data_version""".stripMargin)) { stmt =>
      stmt.setDatasetId(1, datasetId)
      using(stmt.executeQuery()) { rs =>
        val result = Set.newBuilder[String]
        while(rs.next()) result += rs.getString(1)
        result.result()
      }
    }
  }
}
