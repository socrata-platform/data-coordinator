package com.socrata.datacoordinator.secondary.sql

import java.sql.Connection
import java.util.UUID

import com.rojoma.simplearm.v2.using
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.id.sql._
import com.socrata.datacoordinator.secondary.{SecondaryJob, SecondaryJobs}

 class SqlSecondaryJobs(conn: Connection, tableName: String) extends SecondaryJobs {

  override def job(jobId: UUID, step: Short): Option[SecondaryJob] = {
    using(conn.prepareStatement(s"SELECT dataset_system_id, store_id FROM $tableName WHERE job_id = ? AND step = ?")) { stmt =>
      stmt.setObject(1, jobId)
      stmt.setShort(2, step)
      using(stmt.executeQuery()) { rs =>
        if (rs.next()) Some(SecondaryJob(jobId, step, rs.getString("store_id"), rs.getDatasetId("dataset_system_id")))
        else None
      }
    }
  }

  override def jobs(storeId: String, datasetId: DatasetId): Seq[SecondaryJob] = {
    using(conn.prepareStatement(s"SELECT job_id, step FROM $tableName WHERE store_id = ? AND dataset_system_id = ?")) { stmt =>
      stmt.setString(1, storeId)
      stmt.setDatasetId(2, datasetId)
      using(stmt.executeQuery()) { rs =>
        val result = Seq.newBuilder[SecondaryJob]
        while (rs.next()) {
          result += SecondaryJob(rs.getObject("job_id").asInstanceOf[UUID], rs.getShort("step"), storeId, datasetId)
        }
        result.result()
      }
    }
  }

  override def addJob(storeId: String, datasetId: DatasetId, jobId: UUID, step: Short): Unit =
    using(conn.prepareStatement(
      s"INSERT INTO $tableName (store_id, dataset_system_id, job_id, step) VALUES (?, ?, ?, ?)")) { stmt =>
      stmt.setString(1, storeId)
      stmt.setDatasetId(2, datasetId)
      stmt.setObject(3, jobId)
      stmt.setShort(4, step)
      stmt.execute()
    }

  override def deleteJobs(storeId: String, datasetId: DatasetId): Unit =
    using(conn.prepareStatement(
      s"DELETE FROM $tableName WHERE store_id = ? AND dataset_system_id = ?")) { stmt =>
      stmt.setString(1, storeId)
      stmt.setDatasetId(2, datasetId)
      stmt.execute()
    }

   override def deleteJobs(jobId: UUID, step: Short): Unit =
     using(conn.prepareStatement(
       s"DELETE FROM $tableName WHERE job_id = ? AND step = ?")) { stmt =>
       stmt.setObject(1, jobId)
       stmt.setShort(2, step)
       stmt.execute()
     }
 }
