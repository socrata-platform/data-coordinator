package com.socrata.datacoordinator.secondary.sql

import java.sql.Connection
import java.util.{Calendar, Date}

import com.rojoma.simplearm.v2.using
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.id.sql._
import com.socrata.datacoordinator.secondary.SecondaryMetric
import com.socrata.datacoordinator.util.PostgresUniqueViolation
import org.apache.commons.lang3.time.DateUtils

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
        using(conn.prepareStatement(
          """INSERT INTO secondary_metrics (store_id, dataset_system_id, total_size)
            |     VALUES (?, ?, ?)""".stripMargin)) { stmt =>
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

    val date = DateUtils.truncate(new Date(), Calendar.DATE)

    val historyUpdated = using(conn.prepareStatement(
      """UPDATE secondary_metrics_history
            SET total_size = ?
          WHERE store_id = ? AND dataset_system_id = ? AND date = ?
      """)) { stmt =>
        stmt.setLong(1, metric.totalSizeBytes)
        stmt.setString(2, storeId)
        stmt.setDatasetId(3, datasetId)
        stmt.setDate(4, new java.sql.Date(date.getTime))
        stmt.executeUpdate()
      }

    if (historyUpdated == 0) {
      // there should only be one thread trying to due this at a time since this is only called in the SecondaryWatcher
      try {
        using(conn.prepareStatement(
          """INSERT INTO secondary_metrics_history(store_id, dataset_system_id, date, total_size)
                  VALUES (?, ?, ?, ?)
          """)) { stmt =>
          stmt.setString(1, storeId)
          stmt.setDatasetId(2, datasetId)
          stmt.setDate(3, new java.sql.Date(date.getTime))
          stmt.setLong(4, metric.totalSizeBytes)
          stmt.execute()
        }
      } catch {
        case uv@PostgresUniqueViolation(_*) =>
            // there should only be one thread trying to due this at a time since this is only called in the SecondaryWatcher
            log.error("Inserting into secondary_metrics_history failed after updating no rows", uv)
            throw new Exception("Unexpected Postgres unique constraint violation!", uv)
      }
    }

// TODO: replace this with using Postgres UPSERT functionality introduced in 9.5
//    using(conn.prepareStatement(
//      """INSERT INTO secondary_metrics_history(store_id, dataset_system_id, date, total_size)
//                VALUES (?, ?, ?, ?)
//                ON CONFLICT(store_id, dataset_system_id, date)
//                DO UPDATE SET total_size = ?
//      """)) { stmt =>
//      stmt.setString(1, storeId)
//      stmt.setDatasetId(2, datasetId)
//      stmt.setDate(3, new java.sql.Date(date.getTime))
//      stmt.setLong(4, metric.totalSizeBytes)
//      stmt.setLong(5, metric.totalSizeBytes)
//      stmt.execute()
//    }
  }

}
