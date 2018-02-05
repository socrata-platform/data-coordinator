package com.socrata.datacoordinator.secondary.sql

import java.sql.Connection

import com.rojoma.simplearm.v2.using
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.id.sql._
import com.socrata.datacoordinator.secondary.SecondaryMetric
import com.socrata.datacoordinator.util.PostgresUniqueViolation

class PostgresSecondaryMetrics(conn: Connection) extends SqlSecondaryMetrics(conn) {


  private val log = org.slf4j.LoggerFactory.getLogger(classOf[PostgresSecondaryMetrics])

  override def upsertDataset(storeId: String, datasetId: DatasetId, metric: SecondaryMetric): Unit = {
    val rowsUpdated = using(conn.prepareStatement(
      """UPDATE secondary_metrics
        |   SET total_size = ?, updated_at = now()
        | WHERE store_id = ?
        |   AND dataset_system_id = ?""".stripMargin)) { stmt =>
      stmt.setLong(1, metric.totalSizeBytes)
      stmt.setString(2, storeId)
      stmt.setDatasetId(3, datasetId)
      stmt.executeUpdate()
    }

    // TODO: replace this with using Postgres UPSERT functionality introduced in 9.5
    // once we are no longer running 9.4 truth instances
    if (rowsUpdated == 0) {
      try {
        using(conn.prepareStatement("INSERT INTO secondary_metrics (store_id, dataset_system_id, total_size) (?, ?, ?)")) { stmt =>
          stmt.setString(1, storeId)
          stmt.setDatasetId(2, datasetId)
          stmt.setLong(3, metric.totalSizeBytes)
          stmt.execute()
        }
      } catch {
        case uv@PostgresUniqueViolation(_*) =>
          // there should only be one thread trying to due this at a time since this is only called in the SecondaryWatcher
          log.error("Inserting into secondary_metrics failed after updating no rows", uv)
          throw new Exception("Unexpected Postgres unique constraint violation!", uv)
      }
    }
  }
}
