package com.socrata.datacoordinator.secondary.sql

import java.sql.{Connection, PreparedStatement}
import java.util.UUID

import com.rojoma.simplearm.v2.using
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.id.sql._
import com.socrata.datacoordinator.secondary.{SecondaryMoveJob, SecondaryMoveJobs}

class SqlSecondaryMoveJobs(conn: Connection) extends SecondaryMoveJobs {
  val tableName = "secondary_move_jobs"

  private def selectJobs(where: String,
                         includeCompleted: Boolean)(setParams: PreparedStatement => Unit): Seq[SecondaryMoveJob] = {
    val filterCompleted = " AND (move_from_store_completed_at IS NULL OR move_to_store_completed_at IS NULL)"
    val clause = if (includeCompleted) where else where + filterCompleted

    using(conn.prepareStatement(s"SELECT * FROM $tableName WHERE $clause")) { stmt =>
      setParams(stmt)
      using(stmt.executeQuery()) { rs =>
        val result = Seq.newBuilder[SecondaryMoveJob]
        while (rs.next()) {
          result += SecondaryMoveJob(
            id = rs.getObject("job_id").asInstanceOf[UUID],
            datasetId = rs.getDatasetId("dataset_system_id"),
            fromStoreId = rs.getString("from_store_id"),
            toStoreId = rs.getString("to_store_id"),
            moveFromStoreComplete = Option(rs.getTimestamp("move_from_store_completed_at")).isDefined,
            moveToStoreComplete = Option(rs.getTimestamp("move_to_store_completed_at")).isDefined,
            rs.getTimestamp("created_at").getTime
          )
        }
        result.result()
      }
    }
  }

  override def jobs(jobId: UUID): Seq[SecondaryMoveJob] = {
    selectJobs(where = "job_id = ?", includeCompleted = true) { stmt => stmt.setObject(1, jobId) }
  }

  override def jobs(datasetId: DatasetId, includeCompleted: Boolean = false): Seq[SecondaryMoveJob] = {
    selectJobs(where = "dataset_system_id = ?", includeCompleted) { stmt => stmt.setDatasetId(1, datasetId) }
  }

  override def jobsFromStore(storeId: String, datasetId: DatasetId, includeCompleted: Boolean = false): Seq[SecondaryMoveJob] = {
    selectJobs(where = "dataset_system_id = ? AND from_store_id = ?", includeCompleted) { stmt =>
      stmt.setDatasetId(1, datasetId)
      stmt.setString(2, storeId)
    }
  }

  override def jobsToStore(storeId: String, datasetId: DatasetId, includeCompleted: Boolean = false): Seq[SecondaryMoveJob] = {
    selectJobs(where = "dataset_system_id = ? AND to_store_id = ?", includeCompleted) { stmt =>
      stmt.setDatasetId(1, datasetId)
      stmt.setString(2, storeId)
    }
  }

  override def addJob(jobId: UUID, datasetId: DatasetId, fromStoreId: String, toStoreId: String): Unit =
    using(conn.prepareStatement(
      s"INSERT INTO $tableName (job_id, dataset_system_id, from_store_id, to_store_id) VALUES (?, ?, ?, ?)")) { stmt =>
      stmt.setObject(1, jobId)
      stmt.setDatasetId(2, datasetId)
      stmt.setString(3, fromStoreId)
      stmt.setString(4, toStoreId)
      stmt.execute()
    }

  private def updateJobs(set: String, where: String)(setup: PreparedStatement => Unit): Unit = {
    using(conn.prepareStatement(
      s"UPDATE $tableName SET $set WHERE $where")) { stmt =>
      setup(stmt)
      stmt.execute()
    }
  }
  override def markJobsFromStoreComplete(storeId: String, datasetId: DatasetId): Unit = {
    updateJobs(set = "move_from_store_completed_at = now()",
               where = "dataset_system_id = ? AND from_store_id = ? AND move_from_store_completed_at IS NULL") { stmt =>
      stmt.setDatasetId(1, datasetId)
      stmt.setString(2, storeId)
    }
  }

  override def markJobsToStoreComplete(storeId: String, datasetId: DatasetId): Unit = {
    updateJobs(set = "move_to_store_completed_at = now()",
               where = "dataset_system_id = ? AND to_store_id = ? AND move_to_store_completed_at IS NULL") { stmt =>
      stmt.setDatasetId(1, datasetId)
      stmt.setString(2, storeId)
    }
  }

  override def deleteJob(jodId: UUID, datasetId: DatasetId, fromStoreId: String, toStoreId: String): Unit = {
    using(conn.prepareStatement(
      s"""DELETE FROM $tableName
         |      WHERE job_id = ?
         |      AND dataset_system_id = ?
         |      AND from_store_id = ?
         |      AND to_store_id = ?""".stripMargin)) { stmt =>
      stmt.setObject(1, jodId)
      stmt.setDatasetId(2, datasetId)
      stmt.setString(3, fromStoreId)
      stmt.setString(4, toStoreId)
      stmt.execute()
    }
  }
}
