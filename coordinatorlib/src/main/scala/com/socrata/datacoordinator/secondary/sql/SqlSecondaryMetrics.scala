package com.socrata.datacoordinator.secondary.sql

import java.sql.Connection

import com.rojoma.simplearm.v2.using
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.id.sql._
import com.socrata.datacoordinator.secondary.{SecondaryMetric, SecondaryMetrics}

abstract class SqlSecondaryMetrics(conn: Connection) extends SecondaryMetrics {
  override def storeTotal(storeId: String): SecondaryMetric = {
    using(conn.prepareStatement("SELECT sum(total_size) AS total_size FROM secondary_metrics WHERE store_id = ?")) { stmt =>
      stmt.setString(1, storeId)
      using(stmt.executeQuery()) { rs =>
        rs.next()
        SecondaryMetric(
          totalSizeBytes = Option(rs.getLong("total_size")).getOrElse(0L)
        )
      }
    }
  }

  override def dataset(storeId: String, datasetId: DatasetId): Option[SecondaryMetric] = {
    using(conn.prepareStatement(
      """SELECT total_size
        |  FROM secondary_metrics
        | WHERE store_id = ?
        |   AND dataset_system_id = ?""".stripMargin)) { stmt =>
      stmt.setString(1, storeId)
      stmt.setDatasetId(2, datasetId)
      using(stmt.executeQuery()) { rs =>
        if (rs.next()) {
          Some(SecondaryMetric(
            totalSizeBytes = rs.getLong("total_size")
          ))
        } else {
          None
        }
      }
    }
  }

  override def dropDataset(storeId: String, datasetId: DatasetId): Unit = {
    using(conn.prepareStatement("DELETE FROM secondary_metrics WHERE store_id = ? AND dataset_system_id = ?")) { stmt =>
      stmt.setString(1, storeId)
      stmt.setDatasetId(2, datasetId)
      stmt.execute()
    }
  }
}
