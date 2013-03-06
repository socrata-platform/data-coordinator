package com.socrata.datacoordinator.secondary
package sql

import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.id.DatasetId

class SqlSecondaryManifest(conn: Connection, storeId: String) extends SecondaryManifest {
  def lastDataInfo(datasetId: DatasetId): (Long, Option[String]) =
    using(conn.prepareStatement("SELECT version, cookie FROM secondary_manfiest WHERE store_id = ? AND dataset_id = ?")) { stmt =>
      stmt.setString(1, storeId)
      stmt.setLong(2, datasetId.underlying)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          (rs.getLong("version"), Option(rs.getString("cookie")))
        } else {
          using(conn.prepareStatement("INSERT INTO secondary_manifest (store_id, dataset_id, version, cookie) VALUES (?, ?, 0, NULL)")) { stmt2 =>
            stmt2.setString(1, storeId)
            stmt2.setLong(2, datasetId.underlying)
            stmt.execute()
            (0L, None)
          }
        }
      }
    }

  def updateDataInfo(datasetId: DatasetId, dataVersion: Long, cookie: Option[String]) {
    using(conn.prepareStatement("UPDATE secondary_manifest SET version = ?, cookie = ? WHERE store_id = ? AND dataset_id = ?")) { stmt =>
      stmt.setLong(1, dataVersion)
      cookie match {
        case Some(c) => stmt.setString(2, c)
        case None => stmt.setNull(2, java.sql.Types.VARCHAR)
      }
      stmt.setString(3, storeId)
      stmt.setLong(4, datasetId.underlying)
      stmt.execute()
    }
  }

  def dropDataset(datasetId: DatasetId) {
    using(conn.prepareStatement("DELETE FROM secondary_manifest WHERE store_id = ? AND dataset_id = ?")) { stmt =>
      stmt.setString(1, storeId)
      stmt.setLong(2, datasetId.underlying)
      stmt.execute()
    }
  }
}
