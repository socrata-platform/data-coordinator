package com.socrata.datacoordinator.secondary
package sql

import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.id.sql._
import scala.collection.immutable.VectorBuilder

class SqlSecondaryManifest(conn: Connection) extends SecondaryManifest {
  def readLastDatasetInfo(storeId: String, datasetId: DatasetId): Option[(Long, Option[String])] =
    using(conn.prepareStatement("SELECT version, cookie FROM secondary_manifest WHERE store_id = ? AND dataset_system_id = ?")) { stmt =>
      stmt.setString(1, storeId)
      stmt.setDatasetId(2, datasetId)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          Some((rs.getLong("version"), Option(rs.getString("cookie"))))
        } else {
          None
        }
      }
    }

  def lastDataInfo(storeId: String, datasetId: DatasetId): (Long, Option[String]) =
    readLastDatasetInfo(storeId, datasetId).getOrElse {
      using(conn.prepareStatement("INSERT INTO secondary_manifest (store_id, dataset_system_id) VALUES (?, ?)")) { stmt =>
        stmt.setString(1, storeId)
        stmt.setDatasetId(2, datasetId)
        stmt.execute()
        (0L, None)
      }
    }

  def updateDataInfo(storeId: String, datasetId: DatasetId, dataVersion: Long, cookie: Option[String]) {
    using(conn.prepareStatement("UPDATE secondary_manifest SET latest_secondary_data_version = ?, cookie = ? WHERE store_id = ? AND dataset_system_id = ?")) { stmt =>
      stmt.setLong(1, dataVersion)
      cookie match {
        case Some(c) => stmt.setString(2, c)
        case None => stmt.setNull(2, java.sql.Types.VARCHAR)
      }
      stmt.setString(3, storeId)
      stmt.setDatasetId(4, datasetId)
      stmt.execute()
    }
  }

  def dropDataset(storeId: String, datasetId: DatasetId) {
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

  def findDatasetsNeedingReplication(storeId: String, limit: Int): Seq[SecondaryRecord] =
    using(conn.prepareStatement("SELECT dataset_system_id, latest_secondary_data_version, latest_data_version FROM secondary_manifest WHERE store_id = ? AND latest_data_version > latest_secondary_data_version ORDER BY went_out_of_sync_at LIMIT ?")) { stmt =>
      stmt.setString(1, storeId)
      stmt.setInt(1, limit)
      using(stmt.executeQuery()) { rs =>
        val results = new VectorBuilder[SecondaryRecord]
        while(rs.next()) {
          results += SecondaryRecord(
            storeId,
            rs.getDatasetId("dataset_system_id"),
            startingDataVersion = rs.getLong("latest_secondary_data_version") + 1,
            endingDataVersion = rs.getLong("latest_data_version")
          )
        }
        results.result()
      }
    }

  def completedReplicationTo(storeId: String, datasetId: DatasetId, dataVersion: Long) {
    using(conn.prepareStatement("UPDATE secondary_manifest SET latest_secondary_data_version = ?, went_out_of_sync_at = CURRENT_TIMESTAMP WHERE store_id = ? AND dataset_system_id = ?")) { stmt =>
      stmt.setLong(1, dataVersion)
      stmt.setString(2, storeId)
      stmt.setDatasetId(3, datasetId)
      stmt.executeUpdate()
    }
  }
}
