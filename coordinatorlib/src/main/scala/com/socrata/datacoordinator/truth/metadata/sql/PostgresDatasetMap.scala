package com.socrata.datacoordinator
package truth.metadata
package sql

import java.sql.{PreparedStatement, Connection}

import org.postgresql.util.PSQLException
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.{DatasetSystemIdInUseByWriterException, DatasetIdInUseByWriterException}
import com.socrata.datacoordinator.id.{RowId, ColumnId, VersionId, DatasetId}
import com.socrata.datacoordinator.util.PostgresUniqueViolation
import com.socrata.datacoordinator.util.collection.MutableColumnIdMap

class PostgresDatasetMap(conn: Connection) extends DatasetMap with BackupDatasetMap {
  def lockNotAvailableState = "55P03"

  type DatasetInfo = com.socrata.datacoordinator.truth.metadata.DatasetInfo
  type VersionInfo = com.socrata.datacoordinator.truth.metadata.VersionInfo
  type ColumnInfo = com.socrata.datacoordinator.truth.metadata.ColumnInfo

  require(!conn.getAutoCommit, "Connection is in auto-commit mode")

  def snapshotCountQuery = "SELECT count(system_id) FROM version_map WHERE dataset_system_id = ? AND lifecycle_stage = CAST(? AS dataset_lifecycle_stage)"
  def snapshotCount(dataset: DatasetInfo) =
    using(conn.prepareStatement(snapshotCountQuery)) { stmt =>
      stmt.setLong(1, dataset.systemId.underlying)
      stmt.setString(2, LifecycleStage.Snapshotted.name)
      using(stmt.executeQuery()) { rs =>
        rs.next()
        rs.getInt(1)
      }
    }

  def latestQuery = "SELECT system_id, lifecycle_version, lifecycle_stage :: TEXT FROM version_map WHERE dataset_system_id = ? ORDER BY lifecycle_version DESC LIMIT 1"
  def latest(datasetInfo: DatasetInfo) =
    using(conn.prepareStatement(latestQuery)) { stmt =>
      stmt.setLong(1, datasetInfo.systemId.underlying)
      using(stmt.executeQuery()) { rs =>
        if(!rs.next()) sys.error("Looked up a table for " + datasetInfo.datasetId + " but didn't find any version info?")
        VersionInfo(datasetInfo, new VersionId(rs.getLong("system_id")), rs.getLong("lifecycle_version"), LifecycleStage.valueOf(rs.getString("lifecycle_stage")))
      }
    }

  def lookupQuery = "SELECT system_id, lifecycle_version FROM version_map WHERE dataset_system_id = ? AND lifecycle_stage = CAST(? AS dataset_lifecycle_stage) ORDER BY lifecycle_version DESC OFFSET ? LIMIT 1"
  def lookup(datasetInfo: DatasetInfo, stage: LifecycleStage, nth: Int = 0) = {
    using(conn.prepareStatement(lookupQuery)) { stmt =>
      stmt.setLong(1, datasetInfo.systemId.underlying)
      stmt.setString(2, stage.name)
      stmt.setInt(3, nth)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          Some(VersionInfo(datasetInfo, new VersionId(rs.getLong("system_id")), rs.getLong("lifecycle_version"), stage))
        } else {
          None
        }
      }
    }
  }

  def versionQuery = "SELECT system_id, lifecycle_stage FROM version_map WHERE dataset_system_id = ? AND lifecycle_version = ?"
  def version(datasetInfo: DatasetInfo, lifecycleVersion: Long) =
    using(conn.prepareStatement(versionQuery)) { stmt =>
      stmt.setLong(1, datasetInfo.systemId.underlying)
      stmt.setLong(2, lifecycleVersion)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          Some(VersionInfo(datasetInfo, new VersionId(rs.getLong("system_id")), lifecycleVersion, LifecycleStage.valueOf(rs.getString("lifecycle_stage"))))
        } else {
          None
        }
      }
    }

  def schemaQuery = "SELECT system_id, logical_column, type_name, physical_column_base_base, (is_user_primary_key IS NOT NULL) is_user_primary_key FROM column_map WHERE version_system_id = ?"
  def schema(versionInfo: VersionInfo) = {
    using(conn.prepareStatement(schemaQuery)) { stmt =>
      stmt.setLong(1, versionInfo.systemId.underlying)
      using(stmt.executeQuery()) { rs =>
        val result = new MutableColumnIdMap[ColumnInfo]
        while(rs.next()) {
          val systemId = new ColumnId(rs.getLong("system_id"))
          result += systemId -> ColumnInfo(versionInfo, systemId, rs.getString("logical_column"), rs.getString("type_name"), rs.getString("physical_column_base_base"), rs.getBoolean("is_user_primary_key"))
        }
        result.freeze()
      }
    }
  }

  // These are from the reader trait but they're used in the writer tests
  def unpublished(datasetInfo: DatasetInfo) =
    lookup(datasetInfo, LifecycleStage.Unpublished)

  def published(datasetInfo: DatasetInfo) =
    lookup(datasetInfo, LifecycleStage.Published)

  def snapshot(datasetInfo: DatasetInfo, age: Int) =
    lookup(datasetInfo, LifecycleStage.Snapshotted, age)

  def datasetInfoByUserIdQuery = "SELECT system_id, dataset_id, table_base_base, next_row_id FROM dataset_map WHERE dataset_id = ? FOR UPDATE NOWAIT"
  def datasetInfo(datasetId: String) =
    using(conn.prepareStatement(datasetInfoByUserIdQuery)) { stmt =>
      stmt.setString(1, datasetId)
      try {
        extractDatasetInfoFromResultSet(stmt)
      } catch {
        case e: PSQLException if e.getSQLState == lockNotAvailableState =>
          throw new DatasetIdInUseByWriterException(datasetId, e)
      }
    }

  def extractDatasetInfoFromResultSet(stmt: PreparedStatement) =
    using(stmt.executeQuery) { rs =>
      if(rs.next()) {
        Some(DatasetInfo(new DatasetId(rs.getLong("system_id")), rs.getString("dataset_id"), rs.getString("table_base_base"), new RowId(rs.getLong("next_row_id"))))
      } else {
        None
      }
    }

  def datasetInfoBySystemIdQuery          = "SELECT system_id, dataset_id, table_base_base, next_row_id FROM dataset_map WHERE system_id = ?"
  def datasetInfoBySystemIdForUpdateQuery = "SELECT system_id, dataset_id, table_base_base, next_row_id FROM dataset_map WHERE system_id = ? FOR UPDATE NOWAIT"
  def datasetInfo(datasetId: DatasetId) = {
    val query = if(conn.isReadOnly) datasetInfoBySystemIdQuery else datasetInfoBySystemIdForUpdateQuery
    using(conn.prepareStatement(query)) { stmt =>
      stmt.setLong(1, datasetId.underlying)
      try {
        extractDatasetInfoFromResultSet(stmt)
      } catch {
        case e: PSQLException if e.getSQLState == lockNotAvailableState =>
          throw new DatasetSystemIdInUseByWriterException(datasetId, e)
      }
    }
  }

  def createQuery_tableMap = "INSERT INTO dataset_map (dataset_id, table_base_base, next_row_id) VALUES (?, ?, ?) RETURNING system_id"
  def createQuery_versionMap = "INSERT INTO version_map (dataset_system_id, lifecycle_version, lifecycle_stage) VALUES (?, ?, CAST(? AS dataset_lifecycle_stage)) RETURNING system_id"
  def create(datasetId: String, tableBaseBase: String): VersionInfo = {
    val datasetInfo = using(conn.prepareStatement(createQuery_tableMap)) { stmt =>
      val datasetInfoNoSystemId = DatasetInfo(new DatasetId(-1), datasetId, tableBaseBase, RowId.initial)
      stmt.setString(1, datasetInfoNoSystemId.datasetId)
      stmt.setString(2, datasetInfoNoSystemId.tableBaseBase)
      stmt.setLong(3, datasetInfoNoSystemId.nextRowId.underlying)
      try {
        using(stmt.executeQuery()) { rs =>
          val foundSomething = rs.next()
          assert(foundSomething, "INSERT didn't return a system id?")
          datasetInfoNoSystemId.copy(systemId = new DatasetId(rs.getLong(1)))
        }
      } catch {
        case PostgresUniqueViolation("table_base_base") =>
          throw new DatasetAlreadyExistsException(datasetId)
      }
    }

    using(conn.prepareStatement(createQuery_versionMap)) { stmt =>
      val versionInfoNoSystemId = VersionInfo(datasetInfo, new VersionId(-1), 1, LifecycleStage.Unpublished)

      stmt.setLong(1, versionInfoNoSystemId.datasetInfo.systemId.underlying)
      stmt.setLong(2, versionInfoNoSystemId.lifecycleVersion)
      stmt.setString(3, versionInfoNoSystemId.lifecycleStage.name)
      using(stmt.executeQuery()) { rs =>
        val foundSomething = rs.next()
        assert(foundSomething, "Didn't return a system ID?")
        versionInfoNoSystemId.copy(systemId = new VersionId(rs.getLong(1)))
      }
    }
  }

  def createQuery_tableMapWithSystemId = "INSERT INTO dataset_map (system_id, dataset_id, table_base_base) VALUES (?, ?, ?)"
  def createQuery_versionMapWithSystemId = "INSERT INTO version_map (system_id, dataset_system_id, lifecycle_version, lifecycle_stage) VALUES (?, ?, ?, CAST(? AS dataset_lifecycle_stage))"
  def createWithId(systemId: DatasetId, datasetId: String, tableBaseBase: String, initialVersionId: VersionId): VersionInfo = {
    val datasetInfo = DatasetInfo(systemId, datasetId, tableBaseBase, RowId.initial)
    using(conn.prepareStatement(createQuery_tableMapWithSystemId)) { stmt =>
      stmt.setLong(1, datasetInfo.systemId.underlying)
      stmt.setString(2, datasetInfo.datasetId)
      stmt.setString(3, datasetInfo.tableBaseBase)
      try {
        stmt.execute()
      } catch {
        case PostgresUniqueViolation("system_id") =>
          throw new DatasetSystemIdAlreadyInUse(systemId)
        case PostgresUniqueViolation("table_base_base") =>
          throw new DatasetAlreadyExistsException(datasetId)
      }
    }

    using(conn.prepareStatement(createQuery_versionMapWithSystemId)) { stmt =>
      val versionInfo = VersionInfo(datasetInfo, initialVersionId, 1, LifecycleStage.Unpublished)

      stmt.setLong(1, versionInfo.systemId.underlying)
      stmt.setLong(2, versionInfo.datasetInfo.systemId.underlying)
      stmt.setLong(3, versionInfo.lifecycleVersion)
      stmt.setString(4, versionInfo.lifecycleStage.name)
      try {
        stmt.execute()
      } catch {
        case PostgresUniqueViolation("system_id") =>
          throw new VersionSystemIdAlreadyInUse(initialVersionId)
      }

      versionInfo
    }
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

  // like file descriptors, new columns always get the smallest available ID
  def firstFreeColumnIdQuery =
    """-- Adapted from http://johtopg.blogspot.com/2010/07/smallest-available-id.html
      |-- Use zero if available
      |(SELECT
      |    0 AS next_system_id
      | WHERE
      |    NOT EXISTS
      |        (SELECT 1 FROM column_map WHERE system_id = 0 AND version_system_id = ?) )
      |
      |    UNION ALL
      |
      |-- Find the smallest available ID inside a gap, or max + 1
      |-- if there are no gaps.
      |(SELECT
      |    system_id + 1 AS next_system_id
      | FROM
      | (
      |    SELECT
      |        system_id, lead(system_id) OVER (ORDER BY system_id)
      |    FROM
      |        column_map
      |    WHERE
      |        version_system_id = ?
      | ) ss
      | WHERE
      |    lead - system_id > 1 OR
      |    lead IS NULL
      | ORDER BY
      |    system_id
      | LIMIT
      |    1)
      |
      |ORDER BY
      |    next_system_id
      |LIMIT
      |    1
      |""".stripMargin
  def addColumnQuery = "INSERT INTO column_map (system_id, version_system_id, logical_column, type_name, physical_column_base_base) VALUES (?, ?, ?, ?, ?)"
  def addColumn(versionInfo: VersionInfo, logicalName: String, typeName: String, physicalColumnBaseBase: String): ColumnInfo = {
    val systemId = using(conn.prepareStatement(firstFreeColumnIdQuery)) { stmt =>
      stmt.setLong(1, versionInfo.systemId.underlying)
      stmt.setLong(2, versionInfo.systemId.underlying)
      using(stmt.executeQuery()) { rs =>
        val foundSomething = rs.next()
        assert(foundSomething, "Finding the last column info didn't return anything?")
        new ColumnId(rs.getLong("next_system_id"))
      }
    }

    addColumnWithId(systemId, versionInfo, logicalName, typeName, physicalColumnBaseBase)
  }

  def addColumnWithId(systemId: ColumnId, versionInfo: VersionInfo, logicalName: String, typeName: String, physicalColumnBaseBase: String): ColumnInfo = {
    using(conn.prepareStatement(addColumnQuery)) { stmt =>
      val columnInfo = ColumnInfo(versionInfo, systemId, logicalName, typeName, physicalColumnBaseBase, isUserPrimaryKey = false)

      stmt.setLong(1, columnInfo.systemId.underlying)
      stmt.setLong(2, versionInfo.systemId.underlying)
      stmt.setString(3, logicalName)
      stmt.setString(4, typeName)
      stmt.setString(5, physicalColumnBaseBase)
      try {
        stmt.execute()
      } catch {
        case PostgresUniqueViolation("version_system_id", "system_id") =>
          throw new ColumnSystemIdAlreadyInUse(versionInfo, systemId)
        case PostgresUniqueViolation("version_system_id", "logical_column") =>
          throw new ColumnAlreadyExistsException(versionInfo, logicalName)
      }

      columnInfo
    }
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

  def convertColumnQuery = "UPDATE column_map SET type_name = ?, physical_column_base_base = ? WHERE version_system_id = ? AND system_id = ?"
  def convertColumn(columnInfo: ColumnInfo, newType: String, newPhysicalColumnBaseBase: String): ColumnInfo =
    using(conn.prepareStatement(convertColumnQuery)) { stmt =>
      stmt.setString(1, newType)
      stmt.setString(2, newPhysicalColumnBaseBase)
      stmt.setLong(3, columnInfo.versionInfo.systemId.underlying)
      stmt.setLong(4, columnInfo.systemId.underlying)
      val count = stmt.executeUpdate()
      assert(count == 1, "Column did not exist to be converted?")
      columnInfo.copy(typeName = newType, physicalColumnBaseBase = newPhysicalColumnBaseBase)
    }

  def setUserPrimaryKeyQuery = "UPDATE column_map SET is_user_primary_key = 'Unit' WHERE version_system_id = ? AND system_id = ?"
  def setUserPrimaryKey(userPrimaryKey: ColumnInfo) =
    using(conn.prepareStatement(setUserPrimaryKeyQuery)) { stmt =>
      stmt.setLong(1, userPrimaryKey.versionInfo.systemId.underlying)
      stmt.setLong(2, userPrimaryKey.systemId.underlying)
      val count = stmt.executeUpdate()
      assert(count == 1, "Column did not exist to have it set as primary key?")
      userPrimaryKey.copy(isUserPrimaryKey = true)
    }

  def clearUserPrimaryKeyQuery = "UPDATE column_map SET is_user_primary_key = NULL WHERE version_system_id = ?"
  def clearUserPrimaryKey(versionInfo: VersionInfo) {
    using(conn.prepareStatement(clearUserPrimaryKeyQuery)) { stmt =>
      stmt.setLong(1, versionInfo.systemId.underlying)
      stmt.executeUpdate()
    }
  }

  def updateNextRowIdQuery = "UPDATE dataset_map SET next_row_id = ? WHERE system_id = ?"
  def updateNextRowId(datasetInfo: DatasetInfo, newNextRowId: RowId): DatasetInfo = {
    assert(newNextRowId.underlying >= datasetInfo.nextRowId.underlying)
    if(newNextRowId != datasetInfo.nextRowId) {
      using(conn.prepareStatement(updateNextRowIdQuery)) { stmt =>
        stmt.setLong(1, newNextRowId.underlying)
        stmt.setLong(2, datasetInfo.systemId.underlying)
        stmt.executeUpdate()
      }
      datasetInfo.copy(nextRowId = newNextRowId)
    } else {
      datasetInfo
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
  def ensureUnpublishedCopyQueryWithId_versionMap = "INSERT INTO version_map (system_id, dataset_system_id, lifecycle_version, lifecycle_stage) values (?, ?, ?, CAST(? AS dataset_lifecycle_stage))"
  def ensureUnpublishedCopy(tableInfo: DatasetInfo): Either[VersionInfo, CopyPair[VersionInfo]] =
    ensureUnpublishedCopy(tableInfo, None)

  def createUnpublishedCopyWithId(tableInfo: DatasetInfo, versionId: VersionId): CopyPair[VersionInfo] =
    ensureUnpublishedCopy(tableInfo, Some(versionId)).right.getOrElse {
      throw new VersionSystemIdAlreadyInUse(versionId)
    }

  def ensureUnpublishedCopy(tableInfo: DatasetInfo, newVersionId: Option[VersionId]): Either[VersionInfo, CopyPair[VersionInfo]] =
    lookup(tableInfo, LifecycleStage.Unpublished) match {
      case Some(unpublished) =>
        Left(unpublished)
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

            val newVersion = newVersionId match {
              case None =>
                val newVersionWithoutSystemId = publishedCopy.copy(
                  systemId = new VersionId(-1),
                  lifecycleVersion = newLifecycleVersion,
                  lifecycleStage = LifecycleStage.Unpublished)

                val newVersion = using(conn.prepareStatement(ensureUnpublishedCopyQuery_versionMap)) { stmt =>
                  stmt.setLong(1, newVersionWithoutSystemId.datasetInfo.systemId.underlying)
                  stmt.setLong(2, newVersionWithoutSystemId.lifecycleVersion)
                  stmt.setString(3, newVersionWithoutSystemId.lifecycleStage.name)
                  using(stmt.executeQuery()) { rs =>
                    val foundSomething = rs.next()
                    assert(foundSomething, "Insert didn't create a row?")
                    newVersionWithoutSystemId.copy(systemId = new VersionId(rs.getLong(1)))
                  }
                }

                copySchemaIntoUnpublishedCopy(publishedCopy, newVersion)

                newVersion
              case Some(vid) =>
                val newVersion = publishedCopy.copy(
                  systemId = vid,
                  lifecycleVersion = newLifecycleVersion,
                  lifecycleStage = LifecycleStage.Unpublished)

                using(conn.prepareStatement(ensureUnpublishedCopyQuery_versionMap)) { stmt =>
                  stmt.setLong(1, newVersion.systemId.underlying)
                  stmt.setLong(2, newVersion.datasetInfo.systemId.underlying)
                  stmt.setLong(3, newVersion.lifecycleVersion)
                  stmt.setString(4, newVersion.lifecycleStage.name)
                  try {
                    stmt.execute()
                  } catch {
                    case PostgresUniqueViolation("system_id") =>
                      throw new VersionSystemIdAlreadyInUse(vid)
                  }
                }
                newVersion
            }

            Right(CopyPair(publishedCopy, newVersion))
          case None =>
            sys.error("No published copy available?")
        }
    }

  def ensureUnpublishedCopyQuery_columnMap = "INSERT INTO column_map (version_system_id, system_id, logical_column, type_name, physical_column_base_base, is_user_primary_key) SELECT ?, system_id, logical_column, type_name, physical_column_base_base, is_user_primary_key FROM column_map WHERE version_system_id = ?"
  def copySchemaIntoUnpublishedCopy(oldVersion: VersionInfo, newVersion: VersionInfo) {
    using(conn.prepareStatement(ensureUnpublishedCopyQuery_columnMap)) { stmt =>
      stmt.setLong(1, newVersion.systemId.underlying)
      stmt.setLong(2, oldVersion.systemId.underlying)
      stmt.execute()
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
