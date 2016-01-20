package com.socrata.datacoordinator.secondary
package sql

import java.sql.{Types, Connection}
import java.util.UUID

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.id.sql._
import scala.collection.immutable.VectorBuilder
import com.socrata.datacoordinator.truth.metadata
import com.socrata.datacoordinator.util.PostgresUniqueViolation
import scala.concurrent.duration.FiniteDuration

class SqlSecondaryManifest(conn: Connection) extends SecondaryManifest {
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
          |   WHERE dataset_system_id = ? AND lifecycle_stage <> CAST(? AS dataset_lifecycle_stage)
          |   ORDER BY copy_number DESC
          |   LIMIT 1""".stripMargin)) { stmt =>
        stmt.setString(1, storeId)
        stmt.setDatasetId(2, datasetId)
        stmt.setLifecycleStage(3, metadata.LifecycleStage.Discarded)
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
    val job = using(conn.prepareStatement(
      """SELECT dataset_system_id
        |  ,latest_secondary_data_version
        |  ,latest_secondary_lifecycle_stage
        |  ,latest_data_version
        |  ,retry_num
        |  ,cookie
        |FROM secondary_manifest
        |WHERE store_id = ?
        |  AND broken_at IS NULL
        |  AND next_retry <= now()
        |  AND latest_data_version > latest_secondary_data_version
        |  AND (claimant_id is NULL
        |    OR claimed_at < (CURRENT_TIMESTAMP - CAST (? AS INTERVAL)))
        |ORDER BY went_out_of_sync_at
        |LIMIT 1
        |FOR UPDATE""".stripMargin)) { stmt =>
      stmt.setString(1, storeId)
      stmt.setString(2, claimTimeout.toMillis + " milliseconds")
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          val j = SecondaryRecord(
            storeId,
            claimantId,
            rs.getDatasetId("dataset_system_id"),
            startingDataVersion = rs.getLong("latest_secondary_data_version") + 1,
            startingLifecycleStage = rs.getLifecycleStage("latest_secondary_lifecycle_stage"),
            endingDataVersion = rs.getLong("latest_data_version"),
            retryNum = rs.getInt("retry_num"),
            initialCookie = Option(rs.getString("cookie")))
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
        |  ,latest_secondary_lifecycle_stage
        |  ,latest_data_version
        |  ,retry_num
        |  ,cookie
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
            startingLifecycleStage = rs.getLifecycleStage("latest_secondary_lifecycle_stage"),
            endingDataVersion = rs.getLong("latest_data_version"),
            retryNum = rs.getInt("retry_num"),
            initialCookie = Option(rs.getString("cookie")))
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
                             lifecycleStage: metadata.LifecycleStage,
                             cookie: Option[String]): Unit = {
    using(conn.prepareStatement(
      """UPDATE secondary_manifest
        |SET latest_secondary_data_version = ?
        |  ,latest_secondary_lifecycle_stage = CAST(? AS dataset_lifecycle_stage)
        |  ,cookie = ?
        |  ,went_out_of_sync_at = CURRENT_TIMESTAMP
        |WHERE claimant_id = ?
        |  AND store_id = ?
        |  AND dataset_system_id = ?""".stripMargin)) { stmt =>
      stmt.setLong(1, dataVersion)
      stmt.setLifecycleStage(2, lifecycleStage)
      cookie match {
        case Some(c) => stmt.setString(3, c)
        case None => stmt.setNull(3, Types.VARCHAR)
      }
      stmt.setObject(4, claimantId)
      stmt.setString(5, storeId)
      stmt.setDatasetId(6, datasetId)
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
