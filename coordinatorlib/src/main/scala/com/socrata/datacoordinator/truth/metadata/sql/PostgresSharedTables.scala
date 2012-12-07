package com.socrata.datacoordinator.truth.metadata.sql

import java.sql.{Types, Timestamp, Connection}

import org.joda.time.DateTime

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.metadata._

class PostgresSharedTables(conn: Connection, templateEnumsNeedCast: Boolean = true) extends GlobalLog with DatasetMapReader with DatasetMapLifecycleUpdater with DatasetMapSchemaUpdater {
  require(!conn.getAutoCommit, "Connection is in auto-commit mode")

  def log(tableInfo: TableInfo, version: Long, updatedAt: DateTime, updatedBy: String) {
    // bit heavyweight but we want an absolute ordering on these log entries.  In particular,
    // we never want row with id n+1 to become visible to outsiders before row n, even ignoring
    // any other problems.  This is the reason for the "this should be the last thing a txn does"
    // note on the interface.
    using(conn.createStatement()) { stmt =>
      stmt.execute("LOCK TABLE global_log IN EXCLUSIVE MODE")
    }

    using(conn.prepareStatement("INSERT INTO global_log (id, dataset_system_id, version, updated_at, updated_by) SELECT COALESCE(max(id), 0) + 1, ?, ?, ?, ? FROM global_log")) { stmt =>
      stmt.setLong(1, tableInfo.systemId)
      stmt.setLong(2, version)
      stmt.setTimestamp(3, new Timestamp(updatedAt.getMillis))
      stmt.setString(4, updatedBy)
      val count = stmt.executeUpdate()
      assert(count == 1, "Insert into global_log didn't create a row?")
    }
  }

  def tableInfoQuery = "SELECT system_id, dataset_id, table_base FROM table_map WHERE dataset_id = ?"
  def tableInfo(datasetId: String) =
    using(conn.prepareStatement(tableInfoQuery)) { stmt =>
      stmt.setString(1, datasetId)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          Some(TableInfo(rs.getLong("system_id"), rs.getString("dataset_id"), rs.getString("table_base")))
        } else {
          None
        }
      }
    }

  // This is used in the lifecycle stuff when we want to atomically perform an action contingent on
  // some row NOT existing (i.e., making a new unpublished copy).  It's used to guard against phantom
  // read problems.  We _shouldn't_ ever run into this issue, because we'll take a dataset-level
  // zookeeper lock before doing anything, but since it's possible to unknowingly lose a ZK lock...
  // better safe than corrupted.
  def lockTableRowQuery = "SELECT * FROM table_map WHERE system_id = ? FOR UPDATE"
  private def lockTableRow(tableInfo: TableInfo) {
    using(conn.prepareStatement(lockTableRowQuery)) { stmt =>
      stmt.setLong(1, tableInfo.systemId)
      using(stmt.executeQuery()) { rs =>
        val ok = rs.next()
        assert(ok, "Unable to find a row to lock")
      }
    }
  }

  def createQuery_tableMap = "INSERT INTO table_map (dataset_id, table_base) VALUES (?, ?) RETURNING system_id"
  def createQuery_versionMap = "INSERT INTO version_map (dataset_system_id, lifecycle_version, lifecycle_stage) VALUES (?, ?, CAST(? AS dataset_lifecycle_stage))"
  def create(datasetId: String, tableBase: String): VersionInfo = {
    val systemId = using(conn.prepareStatement(createQuery_tableMap)) { stmt =>
      stmt.setString(1, datasetId)
      stmt.setString(2, tableBase)
      using(stmt.executeQuery()) { rs =>
        val returnedSomething = rs.next()
        assert(returnedSomething, "Insert into table_map didn't return an ID?")
        rs.getLong(1)
      }
    }

    val tableInfo = TableInfo(systemId, datasetId, tableBase)
    val versionInfo = VersionInfo(tableInfo, 1, LifecycleStage.Unpublished)

    using(conn.prepareStatement(createQuery_versionMap)) { stmt =>
      stmt.setLong(1, versionInfo.tableInfo.systemId)
      stmt.setLong(2, versionInfo.lifecycleVersion)
      stmt.setString(3, versionInfo.lifecycleStage.name)
      val count = stmt.executeUpdate()
      assert(count == 1, "Insert into version_map didn't create a row?")
    }

    versionInfo
  }

  def deleteQuery_columnMap = "DELETE FROM column_map WHERE dataset_system_id = ?"
  def deleteQuery_versionMap = "DELETE FROM version_map WHERE dataset_system_id = ?"
  def deleteQuery_tableMap = "DELETE FROM table_map WHERE system_id = ?"
  def delete(tableInfo: TableInfo): Boolean = {
    using(conn.prepareStatement(deleteQuery_columnMap)) { stmt =>
      stmt.setLong(1, tableInfo.systemId)
      stmt.executeUpdate()
    }
    using(conn.prepareStatement(deleteQuery_versionMap)) { stmt =>
      stmt.setLong(1, tableInfo.systemId)
      stmt.executeUpdate()
    }
    using(conn.prepareStatement(deleteQuery_tableMap)) { stmt =>
      stmt.setLong(1, tableInfo.systemId)
      val count = stmt.executeUpdate()
      count == 1
    }
  }

  def unpublished(table: TableInfo) =
    lookup(table, LifecycleStage.Unpublished)

  def published(table: TableInfo) =
    lookup(table, LifecycleStage.Published)

  def snapshot(table: TableInfo, age: Int) =
    lookup(table, LifecycleStage.Snapshotted, age)

  def snapshotCountQuery = "SELECT count(*) FROM version_map WHERE dataset_system_id = ? AND lifecycle_stage = CAST(? AS dataset_lifecycle_stage)"
  def snapshotCount(table: TableInfo) =
    using(conn.prepareStatement(snapshotCountQuery)) { stmt =>
      stmt.setLong(1, table.systemId)
      stmt.setString(2, LifecycleStage.Snapshotted.name)
      using(stmt.executeQuery()) { rs =>
        rs.next()
        rs.getInt(1)
      }
    }

  def latestQuery = "SELECT lifecycle_version, lifecycle_stage :: TEXT FROM version_map WHERE dataset_system_id = ? ORDER BY lifecycle_version DESC LIMIT 1"
  def latest(table: TableInfo) =
    using(conn.prepareStatement(latestQuery)) { stmt =>
      stmt.setLong(1, table.systemId)
      using(stmt.executeQuery()) { rs =>
        if(!rs.next()) sys.error("Looked up a table for " + table.datasetId + " but didn't find any version info?")
        VersionInfo(table, rs.getLong("lifecycle_version"), LifecycleStage.valueOf(rs.getString("lifecycle_stage")))
      }
    }

  def lookupQuery = "SELECT lifecycle_version FROM version_map WHERE dataset_system_id = ? AND lifecycle_stage = CAST(? AS dataset_lifecycle_stage) ORDER BY lifecycle_version DESC OFFSET ? LIMIT 1"
  private def lookup(table: TableInfo, stage: LifecycleStage, nth: Int = 0, forUpdate: Boolean = false) = {
    val suffix = if(forUpdate) " FOR UPDATE" else ""
    using(conn.prepareStatement(lookupQuery + suffix)) { stmt =>
      stmt.setLong(1, table.systemId)
      stmt.setString(2, stage.name)
      stmt.setInt(3, nth)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          Some(VersionInfo(table, rs.getLong("lifecycle_version"), stage))
        } else {
          None
        }
      }
    }
  }

  def schemaQuery = "SELECT system_id, logical_column, type_name, physical_column_base, (is_primary_key IS NOT NULL) is_primary_key FROM column_map WHERE dataset_system_id = ? AND lifecycle_version = ?"
  def schema(versionInfo: VersionInfo) = {
    using(conn.prepareStatement(schemaQuery)) { stmt =>
      stmt.setLong(1, versionInfo.tableInfo.systemId)
      stmt.setLong(2, versionInfo.lifecycleVersion)
      using(stmt.executeQuery()) { rs =>
        val result = Map.newBuilder[String, ColumnInfo]
        while(rs.next()) {
          val systemId = rs.getLong("system_id")
          val col = rs.getString("logical_column")
          result += col -> ColumnInfo(versionInfo, systemId, col, rs.getString("type_name"), rs.getString("physical_column_base"), rs.getBoolean("is_primary_key"))
        }
        result.result()
      }
    }
  }

  def addColumnQuery = "INSERT INTO column_map (dataset_system_id, lifecycle_version, logical_column, type_name, physical_column_base) VALUES (?, ?, ?, ?, ?) RETURNING system_id"
  def addColumn(versionInfo: VersionInfo, logicalName: String, typeName: String, physicalColumnBase: String): ColumnInfo =
    using(conn.prepareStatement(addColumnQuery)) { stmt =>
      stmt.setLong(1, versionInfo.tableInfo.systemId)
      stmt.setLong(2, versionInfo.lifecycleVersion)
      stmt.setString(3, logicalName)
      stmt.setString(4, typeName)
      stmt.setString(5, physicalColumnBase)
      val systemId = using(stmt.executeQuery()) { rs =>
        val returnedSomething = rs.next()
        assert(returnedSomething, "Insert into table_map didn't return an ID?")
        rs.getLong(1)
      }

      ColumnInfo(versionInfo, systemId, logicalName, typeName, physicalColumnBase, isPrimaryKey = false)
    }

  def dropColumnQuery = "DELETE FROM column_map WHERE dataset_system_id = ? AND lifecycle_version = ? AND system_id = ?"
  def dropColumn(columnInfo: ColumnInfo) {
    using(conn.prepareStatement(dropColumnQuery)) { stmt =>
      stmt.setLong(1, columnInfo.versionInfo.tableInfo.systemId)
      stmt.setLong(2, columnInfo.versionInfo.lifecycleVersion)
      stmt.setLong(3, columnInfo.systemId)
      val count = stmt.executeUpdate()
      assert(count == 1, "Delete from column_map didn't remove a row?")
    }
  }

  def renameColumnQuery = "UPDATE column_map SET logical_column = ? WHERE dataset_system_id = ? AND lifecycle_version = ? AND system_id = ?"
  def renameColumn(columnInfo: ColumnInfo, newLogicalName: String): ColumnInfo = {
    using(conn.prepareStatement(renameColumnQuery)) { stmt =>
      stmt.setString(1, newLogicalName)
      stmt.setLong(2, columnInfo.versionInfo.tableInfo.systemId)
      stmt.setLong(3, columnInfo.versionInfo.lifecycleVersion)
      stmt.setLong(4, columnInfo.systemId)
      val count = stmt.executeUpdate()
      assert(count == 1, "Rename column in column_map didn't change a row?")
      columnInfo.copy(logicalName =  newLogicalName)
    }
  }

  def convertColumnQuery = "UPDATE column_map SET type_name = ?, physical_column_base = ? WHERE dataset_system_id = ? AND lifecycle_version = ? AND system_id = ?"
  def convertColumn(columnInfo: ColumnInfo, newType: String, newPhysicalColumnBase: String): ColumnInfo = {
    using(conn.prepareStatement(convertColumnQuery)) { stmt =>
      stmt.setString(1, newType)
      stmt.setString(2, newPhysicalColumnBase)
      stmt.setLong(3, columnInfo.versionInfo.tableInfo.systemId)
      stmt.setLong(4, columnInfo.versionInfo.lifecycleVersion)
      stmt.setLong(5, columnInfo.systemId)
      val count = stmt.executeUpdate()
      assert(count == 1, "Change type of column in column_map didn't change a row?")
      columnInfo.copy(typeName = newType, physicalColumnBase = newPhysicalColumnBase)
    }
  }

  def setUserPrimaryKeyQuery = "UPDATE column_map SET is_primary_key = 'Unit' WHERE dataset_system_id = ? AND lifecycle_version = ? AND system_id = ?"
  def setUserPrimaryKey(userPrimaryKey: ColumnInfo) {
    using(conn.prepareStatement(setUserPrimaryKeyQuery)) { stmt =>
      stmt.setLong(1, userPrimaryKey.versionInfo.tableInfo.systemId)
      stmt.setLong(2, userPrimaryKey.versionInfo.lifecycleVersion)
      stmt.setLong(3, userPrimaryKey.systemId)
      val count = stmt.executeUpdate()
      assert(count == 1, "Change primary key in version_map didn't change a row?") // this will be 1 even if it was already a PK
    }
  }

  def clearUserPrimaryKeyQuery = "UPDATE column_map SET is_primary_key = NULL WHERE dataset_system_id = ? AND lifecycle_version = ?"
  def clearUserPrimaryKey(versionInfo: VersionInfo) {
    using(conn.prepareStatement(clearUserPrimaryKeyQuery)) { stmt =>
      stmt.setLong(1, versionInfo.tableInfo.systemId)
      stmt.setLong(2, versionInfo.lifecycleVersion)
      stmt.executeUpdate() // no need to check; we're good even if there wasn't one before
    }
  }

  def dropCopyQuery_columnMap = "DELETE FROM column_map WHERE dataset_system_id = ? AND lifecycle_version = ?"
  def dropCopyQuery_versionMap = "DELETE FROM version_map WHERE dataset_system_id = ? AND lifecycle_version = ? AND lifecycle_stage = CAST(? AS dataset_lifecycle_stage)"
  def dropCopy(versionInfo: VersionInfo): Boolean = {
    if(versionInfo.lifecycleStage != LifecycleStage.Snapshotted && versionInfo.lifecycleStage != LifecycleStage.Unpublished) {
      throw new IllegalArgumentException("Can only drop a snapshot or an unpublished copy of a dataset.")
    }

    using(conn.prepareStatement(dropCopyQuery_columnMap)) { stmt =>
      stmt.setLong(1, versionInfo.tableInfo.systemId)
      stmt.setLong(2, versionInfo.lifecycleVersion)
      stmt.executeUpdate()
    }
    using(conn.prepareStatement(dropCopyQuery_versionMap)) { stmt =>
      stmt.setLong(1, versionInfo.tableInfo.systemId)
      stmt.setLong(2, versionInfo.lifecycleVersion)
      stmt.setString(3, versionInfo.lifecycleStage.name) // just to make sure the user wasn't lying about the stage
      stmt.executeUpdate() == 1
    }
  }

  def ensureUnpublishedCopyQuery_newLifecycleVersion = "SELECT max(lifecycle_version) + 1 FROM version_map WHERE dataset_system_id = ?"
  def ensureUnpublishedCopyQuery_versionMap = "INSERT INTO version_map (dataset_system_id, lifecycle_version, lifecycle_stage) values (?, ?, CAST(? AS dataset_lifecycle_stage), ?)"
  def ensureUnpublishedCopyQuery_columnMap = "INSERT INTO column_map (dataset_system_id, lifecycle_version, logical_column, type_name, physical_column_base, is_primary_key) SELECT dataset_system_id, ?, logical_column, type_name, physical_column_base, is_primary_key FROM column_map WHERE lifecycle_version = ?"
  def ensureUnpublishedCopy(tableInfo: TableInfo): VersionInfo = {
    lockTableRow(tableInfo)
    unpublished(tableInfo) match {
      case Some(unpublished) =>
        unpublished
      case None =>
        published(tableInfo) match {
          case Some(publishedCopy) =>
            val newLifecycleVersion = using(conn.prepareStatement(ensureUnpublishedCopyQuery_newLifecycleVersion)) { stmt =>
              stmt.setLong(1, publishedCopy.tableInfo.systemId)
              using(stmt.executeQuery()) { rs =>
                rs.next()
                rs.getLong(1)
              }
            }

            val newVersion = publishedCopy.copy(
              lifecycleVersion = newLifecycleVersion,
              lifecycleStage = LifecycleStage.Unpublished)

            using(conn.prepareStatement(ensureUnpublishedCopyQuery_versionMap)) { stmt =>
              stmt.setLong(1, newVersion.tableInfo.systemId)
              stmt.setLong(2, newVersion.lifecycleVersion)
              stmt.setString(3, newVersion.lifecycleStage.name)
              val count = stmt.executeUpdate()
              assert(count == 1, "Making an unpublished copy didn't change a row?")
            }

            using(conn.prepareStatement(ensureUnpublishedCopyQuery_columnMap)) { stmt =>
              stmt.setLong(1, newVersion.lifecycleVersion)
              stmt.setLong(2, publishedCopy.lifecycleVersion)
              stmt.execute()
            }

            newVersion
          case None =>
            sys.error("No published copy available?")
        }
    }
  }

  def publishQuery = "UPDATE version_map SET lifecycle_stage = CAST(? AS dataset_lifecycle_stage) WHERE dataset_system_id = ? AND lifecycle_version = ?"
  def publish(tableInfo: TableInfo): Option[VersionInfo] = {
    lookup(tableInfo, LifecycleStage.Unpublished, forUpdate = true) map { workingCopy =>
      using(conn.prepareStatement(publishQuery)) { stmt =>
        for(published <- lookup(tableInfo, LifecycleStage.Published, forUpdate = true)) {
          stmt.setString(1, LifecycleStage.Snapshotted.name)
          stmt.setLong(2, published.tableInfo.systemId)
          stmt.setLong(3, published.lifecycleVersion)
          val count = stmt.executeUpdate()
          assert(count == 1, "Snapshotting a published copy didn't change a row?")
        }
        stmt.setString(1, LifecycleStage.Published.name)
        stmt.setLong(2, workingCopy.tableInfo.systemId)
        stmt.setLong(3, workingCopy.lifecycleVersion)
        val count = stmt.executeUpdate()
        assert(count == 1, "Publishing an unpublished copy didn't change a row?")
        workingCopy.copy(lifecycleStage = LifecycleStage.Published)
      }
    }
  }
}
