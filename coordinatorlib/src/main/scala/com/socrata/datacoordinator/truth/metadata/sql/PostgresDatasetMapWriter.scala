package com.socrata.datacoordinator
package truth.metadata
package sql

import java.sql.{PreparedStatement, Timestamp, Connection}

import org.joda.time.DateTime
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.{DatasetSystemIdInUseByWriterException, DatasetIdInUseByWriterException}
import com.socrata.datacoordinator.id.{ColumnId, VersionId, DatasetId}

class PostgresDatasetMapWriter(_conn: Connection) extends `-impl`.PostgresDatasetMapReaderAPI(_conn) with DatasetMapWriter {
  def lockNotAvailableState = "55P03"

  def datasetInfoByUserIdQuery = "SELECT system_id, dataset_id, table_base FROM dataset_map WHERE dataset_id = ? FOR UPDATE NOWAIT"
  def datasetInfo(datasetId: String) =
    using(conn.prepareStatement(datasetInfoByUserIdQuery)) { stmt =>
      stmt.setString(1, datasetId)
      try {
        extractDatasetInfoFromResultSet(stmt)
      } catch {
        case e: org.postgresql.util.PSQLException if e.getServerErrorMessage.getSQLState == lockNotAvailableState =>
          throw new DatasetIdInUseByWriterException(datasetId, e)
      }
    }

  def extractDatasetInfoFromResultSet(stmt: PreparedStatement) =
    using(stmt.executeQuery) { rs =>
      if(rs.next()) {
        Some(SqlDatasetInfo(new DatasetId(rs.getLong("system_id")), rs.getString("dataset_id"), rs.getString("table_base")))
      } else {
        None
      }
    }

  def datasetInfoBySystemIdQuery = "SELECT system_id, dataset_id, table_base FROM dataset_map WHERE system_id = ? FOR UPDATE NOWAIT"
  def datasetInfo(datasetId: DatasetId) =
    using(conn.prepareStatement(datasetInfoBySystemIdQuery)) { stmt =>
      stmt.setLong(1, datasetId.underlying)
      try {
        extractDatasetInfoFromResultSet(stmt)
      } catch {
        case e: org.postgresql.util.PSQLException if e.getServerErrorMessage.getSQLState == lockNotAvailableState =>
          throw new DatasetSystemIdInUseByWriterException(datasetId, e)
      }
    }

  def createQuery_tableMap = "INSERT INTO dataset_map (dataset_id, table_base) VALUES (?, ?) RETURNING system_id"
  def createQuery_versionMap = "INSERT INTO version_map (dataset_system_id, lifecycle_version, lifecycle_stage) VALUES (?, ?, CAST(? AS dataset_lifecycle_stage)) RETURNING system_id"
  def create(datasetId: String, tableBase: String): VersionInfo = {
    val tableSystemId = using(conn.prepareStatement(createQuery_tableMap)) { stmt =>
      stmt.setString(1, datasetId)
      stmt.setString(2, tableBase)
      using(stmt.executeQuery()) { rs =>
        val returnedSomething = rs.next()
        assert(returnedSomething, "Insert into dataset_map didn't return an ID?")
        new DatasetId(rs.getLong(1))
      }
    }

    val tableInfo = SqlDatasetInfo(tableSystemId, datasetId, tableBase)
    val versionInfo = SqlVersionInfo(tableInfo, new VersionId(Int.MinValue), 1, LifecycleStage.Unpublished)

    val versionSystemId = using(conn.prepareStatement(createQuery_versionMap)) { stmt =>
      stmt.setLong(1, versionInfo.datasetInfo.systemId.underlying)
      stmt.setLong(2, versionInfo.lifecycleVersion)
      stmt.setString(3, versionInfo.lifecycleStage.name)
      using(stmt.executeQuery()) { rs =>
        val returnedSomething = rs.next()
        assert(returnedSomething, "Insert into version_map didn't return an ID?")
        rs.getLong(1)
      }
    }

    versionInfo.copy(systemId = new VersionId(versionSystemId))
  }

  // Yay no "DELETE ... CASCADE"!
  def deleteQuery_columnMap = "DELETE FROM column_map WHERE version_system_id IN (SELECT system_id FROM version_map WHERE dataset_system_id = ?)"
  def deleteQuery_versionMap = "DELETE FROM version_map WHERE dataset_system_id = ?"
  def deleteQuery_tableMap = "DELETE FROM dataset_map WHERE system_id = ?"
  def delete(tableInfo: DatasetInfo) {
    using(conn.prepareStatement(deleteQuery_columnMap)) { stmt =>
      stmt.setLong(1, tableInfo.systemId.underlying)
      stmt.executeUpdate()
    }
    using(conn.prepareStatement(deleteQuery_versionMap)) { stmt =>
      stmt.setLong(1, tableInfo.systemId.underlying)
      stmt.executeUpdate()
    }
    using(conn.prepareStatement(deleteQuery_tableMap)) { stmt =>
      stmt.setLong(1, tableInfo.systemId.underlying)
      val count = stmt.executeUpdate()
      assert(count == 1, "Called delete on a table which is no longer there?")
    }
  }

  def addColumnQuery = "INSERT INTO column_map (version_system_id, logical_column, type_name, physical_column_base) VALUES (?, ?, ?, ?) RETURNING system_id"
  def addColumn(versionInfo: VersionInfo, logicalName: String, typeName: String, physicalColumnBase: String): ColumnInfo =
    using(conn.prepareStatement(addColumnQuery)) { stmt =>
      stmt.setLong(1, versionInfo.systemId.underlying)
      stmt.setString(2, logicalName)
      stmt.setString(3, typeName)
      stmt.setString(4, physicalColumnBase)
      val systemId = using(stmt.executeQuery()) { rs =>
        val returnedSomething = rs.next()
        assert(returnedSomething, "Insert into dataset_map didn't return an ID?")
        rs.getLong(1)
      }

      SqlColumnInfo(versionInfo, new ColumnId(systemId), logicalName, typeName, physicalColumnBase, isPrimaryKey = false)
    }

  def dropColumnQuery = "DELETE FROM column_map WHERE version_system_id = ? AND system_id = ?"
  def dropColumn(columnInfo: ColumnInfo) {
    using(conn.prepareStatement(dropColumnQuery)) { stmt =>
      stmt.setLong(1, columnInfo.versionInfo.systemId.underlying)
      stmt.setLong(2, columnInfo.systemId.underlying)
      val count = stmt.executeUpdate()
      assert(count == 1, "Column did not exist to be dropped?")
    }
  }

  def renameColumnQuery = "UPDATE column_map SET logical_column = ? WHERE version_system_id = ? AND system_id = ?"
  def renameColumn(columnInfo: ColumnInfo, newLogicalName: String): ColumnInfo =
    using(conn.prepareStatement(renameColumnQuery)) { stmt =>
      stmt.setString(1, newLogicalName)
      stmt.setLong(2, columnInfo.versionInfo.systemId.underlying)
      stmt.setLong(3, columnInfo.systemId.underlying)
      val count = stmt.executeUpdate()
      assert(count == 1, "Column did not exist to be renamed?")
      columnInfo.copy(logicalName =  newLogicalName)
    }

  def convertColumnQuery = "UPDATE column_map SET type_name = ?, physical_column_base = ? WHERE version_system_id = ? AND system_id = ?"
  def convertColumn(columnInfo: ColumnInfo, newType: String, newPhysicalColumnBase: String): ColumnInfo =
    using(conn.prepareStatement(convertColumnQuery)) { stmt =>
      stmt.setString(1, newType)
      stmt.setString(2, newPhysicalColumnBase)
      stmt.setLong(3, columnInfo.versionInfo.systemId.underlying)
      stmt.setLong(4, columnInfo.systemId.underlying)
      val count = stmt.executeUpdate()
      assert(count == 1, "Column did not exist to be converted?")
      columnInfo.copy(typeName = newType, physicalColumnBase = newPhysicalColumnBase)
    }

  def setUserPrimaryKeyQuery = "UPDATE column_map SET is_primary_key = 'Unit' WHERE version_system_id = ? AND system_id = ?"
  def setUserPrimaryKey(userPrimaryKey: ColumnInfo) =
    using(conn.prepareStatement(setUserPrimaryKeyQuery)) { stmt =>
      stmt.setLong(1, userPrimaryKey.versionInfo.systemId.underlying)
      stmt.setLong(2, userPrimaryKey.systemId.underlying)
      val count = stmt.executeUpdate()
      assert(count == 1, "Column did not exist to have it set as primary key?")
      userPrimaryKey.copy(isPrimaryKey = true)
    }

  def clearUserPrimaryKeyQuery = "UPDATE column_map SET is_primary_key = NULL WHERE version_system_id = ?"
  def clearUserPrimaryKey(versionInfo: VersionInfo) {
    using(conn.prepareStatement(clearUserPrimaryKeyQuery)) { stmt =>
      stmt.setLong(1, versionInfo.systemId.underlying)
      stmt.executeUpdate()
    }
  }

  def dropCopyQuery_columnMap = "DELETE FROM column_map WHERE version_system_id IN (SELECT system_id FROM version_map WHERE system_id = ? AND lifecycle_stage = CAST(? AS dataset_lifecycle_stage))"
  def dropCopyQuery_versionMap = "DELETE FROM version_map WHERE system_id = ? AND lifecycle_stage = CAST(? AS dataset_lifecycle_stage)"
  def dropCopy(versionInfo: VersionInfo) {
    if(versionInfo.lifecycleStage != LifecycleStage.Snapshotted && versionInfo.lifecycleStage != LifecycleStage.Unpublished) {
      throw new IllegalArgumentException("Can only drop a snapshot or an unpublished copy of a dataset.")
    }
    if(versionInfo.lifecycleStage == LifecycleStage.Unpublished && versionInfo.lifecycleVersion == 1) {
      throw new IllegalArgumentException("Cannot drop the initial version")
    }

    using(conn.prepareStatement(dropCopyQuery_columnMap)) { stmt =>
      stmt.setLong(1, versionInfo.systemId.underlying)
      stmt.setString(2, versionInfo.lifecycleStage.name) // just to make sure the user wasn't mistaken about the stage
      stmt.executeUpdate()
    }

    using(conn.prepareStatement(dropCopyQuery_versionMap)) { stmt =>
      stmt.setLong(1, versionInfo.systemId.underlying)
      stmt.setString(2, versionInfo.lifecycleStage.name) // just to make sure the user wasn't mistaken about the stage
      val count = stmt.executeUpdate()
      assert(count == 1, "Copy did not exist to be dropped?")
    }
  }

  def ensureUnpublishedCopyQuery_newLifecycleVersion = "SELECT max(lifecycle_version) + 1 FROM version_map WHERE dataset_system_id = ?"
  def ensureUnpublishedCopyQuery_versionMap = "INSERT INTO version_map (dataset_system_id, lifecycle_version, lifecycle_stage) values (?, ?, CAST(? AS dataset_lifecycle_stage)) RETURNING system_id"
  def ensureUnpublishedCopyQuery_columnMap = "INSERT INTO column_map (version_system_id, system_id, logical_column, type_name, physical_column_base, is_primary_key) SELECT ?, system_id, logical_column, type_name, physical_column_base, is_primary_key FROM column_map WHERE version_system_id = ?"
  def ensureUnpublishedCopy(tableInfo: DatasetInfo): VersionInfo =
    lookup(tableInfo, LifecycleStage.Unpublished) match {
      case Some(unpublished) =>
        unpublished
      case None =>
        lookup(tableInfo, LifecycleStage.Published) match {
          case Some(publishedCopy) =>
            val newLifecycleVersion = using(conn.prepareStatement(ensureUnpublishedCopyQuery_newLifecycleVersion)) { stmt =>
              stmt.setLong(1, publishedCopy.datasetInfo.systemId.underlying)
              using(stmt.executeQuery()) { rs =>
                rs.next()
                rs.getLong(1)
              }
            }

            val newVersion = using(conn.prepareStatement(ensureUnpublishedCopyQuery_versionMap)) { stmt =>
              val newVersion = publishedCopy.copy(
                lifecycleVersion = newLifecycleVersion,
                lifecycleStage = LifecycleStage.Unpublished)

              stmt.setLong(1, newVersion.datasetInfo.systemId.underlying)
              stmt.setLong(2, newVersion.lifecycleVersion)
              stmt.setString(3, newVersion.lifecycleStage.name)
              using(stmt.executeQuery()) { rs =>
                val returnedSomething = rs.next()
                assert(returnedSomething, "Making an unpublished copy didn't add a row?")
                newVersion.copy(systemId = new VersionId(rs.getLong(1)))
              }
            }

            using(conn.prepareStatement(ensureUnpublishedCopyQuery_columnMap)) { stmt =>
              stmt.setLong(1, newVersion.systemId.underlying)
              stmt.setLong(2, publishedCopy.systemId.underlying)
              stmt.execute()
            }

            newVersion
          case None =>
            sys.error("No published copy available?")
        }
    }

  def publishQuery = "UPDATE version_map SET lifecycle_stage = CAST(? AS dataset_lifecycle_stage) WHERE system_id = ?"
  def publish(unpublishedCopy: VersionInfo): VersionInfo = {
    if(unpublishedCopy.lifecycleStage != LifecycleStage.Unpublished) {
      throw new IllegalArgumentException("Version does not name an unpublished copy")
    }
    using(conn.prepareStatement(publishQuery)) { stmt =>
      for(published <- lookup(unpublishedCopy.datasetInfo, LifecycleStage.Published)) {
        stmt.setString(1, LifecycleStage.Snapshotted.name)
        stmt.setLong(2, published.systemId.underlying)
        val count = stmt.executeUpdate()
        assert(count == 1, "Snapshotting a published copy didn't change a row?")
      }
      stmt.setString(1, LifecycleStage.Published.name)
      stmt.setLong(2, unpublishedCopy.systemId.underlying)
      val count = stmt.executeUpdate()
      assert(count == 1, "Publishing an unpublished copy didn't change a row?")
      unpublishedCopy.copy(lifecycleStage = LifecycleStage.Published)
    }
  }
}
