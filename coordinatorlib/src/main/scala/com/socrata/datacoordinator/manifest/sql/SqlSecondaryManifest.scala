package com.socrata.datacoordinator
package manifest
package sql

import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.util.collection.{StoreIdMap, MutableStoreIdMap}
import com.socrata.datacoordinator.id.{StoreId, DatasetId}

class SqlSecondaryManifest(conn: Connection) extends SecondaryManifest {
  def create(storeId: StoreId, datasetId: DatasetId) {
    using(conn.prepareStatement("INSERT INTO secondary_manifest (store_system_id, dataset_system_id, version) VALUES (?, ?, 0)")) { stmt =>
      stmt.setLong(1, storeId.underlying)
      stmt.setLong(2, datasetId.underlying)
      stmt.execute()
    }
  }

  def updateVersion(storeId: StoreId, datasetId: DatasetId, version: Long) {
    using(conn.prepareStatement("UPDATE secondary_manifest SET version = ? WHERE store_system_id = ? AND dataset_system_id = ?")) { stmt =>
      stmt.setLong(1, version)
      stmt.setLong(2, storeId.underlying)
      stmt.setLong(3, datasetId.underlying)
      val count = stmt.executeUpdate()
      assert(count == 1, "updateVersion didn't update anything")
    }
  }

  def versionOf(storeId: StoreId, datasetId: DatasetId): Option[Long] =
    using(conn.prepareStatement("SELECT version FROM secondary_manifest WHERE store_system_id = ? AND dataset_system_id = ?")) { stmt =>
      stmt.setLong(1, storeId.underlying)
      stmt.setLong(2, datasetId.underlying)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          val version = rs.getLong("version")
          Some(version)
        } else {
          None
        }
      }
    }

  def allVersionsOfDataset(datasetId: DatasetId): StoreIdMap[Long] =
    using(conn.prepareStatement("SELECT store_system_id, version FROM secondary_manifest WHERE dataset_system_id = ?")) { stmt =>
      stmt.setLong(1, datasetId.underlying)
      using(stmt.executeQuery()) { rs =>
        val result = new MutableStoreIdMap[Long]()
        while(rs.next()) {
          val sId = new StoreId(rs.getLong("store_system_id"))
          val ver = rs.getLong("version")
          result(sId) = ver
        }
        result.freeze()
      }
    }

  def remove(storeId: StoreId, datasetId: DatasetId) {
    using(conn.prepareStatement("DELETE FROM secondary_manifest WHERE store_system_id = ? AND dataset_system_id = ?")) { stmt =>
      stmt.setLong(1, storeId.underlying)
      stmt.setLong(2, datasetId.underlying)
      val count = stmt.executeUpdate()
      assert(count == 1, "Remove didn't remove anything?")
    }
  }
}
