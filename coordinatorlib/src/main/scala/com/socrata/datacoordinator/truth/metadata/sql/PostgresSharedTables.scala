package com.socrata.datacoordinator.truth.metadata.sql

import java.sql.{Types, Timestamp, Connection}

import org.joda.time.DateTime

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.metadata._

class PostgresSharedTables(conn: Connection) extends GlobalLog with DatasetMapReader with DatasetMapLifecycleUpdater with DatasetMapSchemaUpdater {
  require(!conn.getAutoCommit, "Connection is in auto-commit mode")

  def log(tableInfo: TableInfo, version: Long, updatedAt: DateTime, updatedBy: String) {
    // bit heavyweight but we want an absolute ordering on these log entries.  In particular,
    // we never want row with id n+1 to become visible to outsiders before row n, even ignoring
    // any other problems.  This is the reason for the "this should be the last thing a txn does"
    // note on the interface.
    using(conn.createStatement()) { stmt =>
      stmt.execute("LOCK TABLE global_log IN EXCLUSIVE MODE")
    }

    using(conn.prepareStatement("INSERT INTO global_log (id, dataset_id, version, updated_at, updated_by) SELECT COALESCE(max(id), 0) + 1, ?, ?, ?, ? FROM global_log")) { stmt =>
      stmt.setString(1, tableInfo.datasetId)
      stmt.setLong(2, version)
      stmt.setTimestamp(3, new Timestamp(updatedAt.getMillis))
      stmt.setString(4, updatedBy)
      val count = stmt.executeUpdate()
      assert(count == 1, "Insert into global_log didn't create a row?")
    }
  }

  def tableInfo(datasetId: String) =
    using(conn.prepareStatement("SELECT system_id, dataset_id, table_base FROM table_map WHERE dataset_id = ?")) { stmt =>
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
  private def lockTableRow(tableInfo: TableInfo) {
    using(conn.prepareStatement("SELECT * FROM table_map WHERE system_id = ? FOR UPDATE")) { stmt =>
      stmt.setLong(1, tableInfo.systemId)
      using(stmt.executeQuery()) { rs =>
        val ok = rs.next()
        assert(ok, "Unable to find a row to lock")
      }
    }
  }

  def create(datasetId: String, tableBase: String, userPrimaryKey: Option[String]): VersionInfo = {
    using(conn.prepareStatement("INSERT INTO table_map (dataset_id, table_base) VALUES (?, ?)")) { stmt =>
      stmt.setString(1, datasetId)
      stmt.setString(2, tableBase)
      val count = stmt.executeUpdate()
      assert(count == 1, "Insert into table_map didn't create a row?")
    }

    val systemId = for {
      stmt <- managed(conn.createStatement())
      rs <- managed(stmt.executeQuery("SELECT currval('table_map_system_id_seq')"))
    } yield {
      rs.next()
      rs.getLong(1)
    }

    val tableInfo = TableInfo(systemId, datasetId, tableBase)
    val versionInfo = VersionInfo(tableInfo, 1, LifecycleStage.Unpublished, userPrimaryKey)

    using(conn.prepareStatement("INSERT INTO version_map (dataset_system_id, lifecycle_version, lifecycle_stage, primary_key) VALUES (?, ?, CAST(? AS dataset_lifecycle_stage), ?)")) { stmt =>
      stmt.setLong(1, versionInfo.tableInfo.systemId)
      stmt.setLong(2, versionInfo.lifecycleVersion)
      stmt.setString(3, versionInfo.lifecycleStage.name)
      versionInfo.userPrimaryKey match {
        case Some(pk) => stmt.setString(4, pk)
        case None => stmt.setNull(4, Types.VARCHAR)
      }
      val count = stmt.executeUpdate()
      assert(count == 1, "Insert into version_map didn't create a row?")
    }

    versionInfo
  }

  def delete(tableInfo: TableInfo): Boolean = {
    using(conn.prepareStatement("DELETE FROM column_map WHERE dataset_system_id = ?")) { stmt =>
      stmt.setLong(1, tableInfo.systemId)
      stmt.executeUpdate()
    }
    using(conn.prepareStatement("DELETE FROM version_map WHERE dataset_system_id = ?")) { stmt =>
      stmt.setLong(1, tableInfo.systemId)
      stmt.executeUpdate()
    }
    using(conn.prepareStatement("DELETE FROM table_map WHERE system_id = ?")) { stmt =>
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

  def snapshotCount(table: TableInfo) =
    using(conn.prepareStatement("SELECT count(*) FROM version_map WHERE dataset_system_id = ? AND lifecycle_stage = CAST(? AS dataset_lifecycle_stage)")) { stmt =>
      stmt.setLong(1, table.systemId)
      stmt.setString(2, LifecycleStage.Snapshotted.name)
      using(stmt.executeQuery()) { rs =>
        rs.next()
        rs.getInt(1)
      }
    }

  def latest(table: TableInfo) =
    using(conn.prepareStatement("SELECT lifecycle_version, lifecycle_stage :: TEXT, primary_key FROM version_map WHERE dataset_system_id = ? ORDER BY lifecycle_version DESC LIMIT 1")) { stmt =>
      stmt.setLong(1, table.systemId)
      using(stmt.executeQuery()) { rs =>
        if(!rs.next()) sys.error("Looked up a table for " + table.datasetId + " but didn't find any version info?")
        VersionInfo(table, rs.getLong("lifecycle_version"), LifecycleStage.valueOf(rs.getString("lifecycle_stage")), Option(rs.getString("primary_key")))
      }
    }

  private def lookup(table: TableInfo, stage: LifecycleStage, nth: Int = 0, forUpdate: Boolean = false) = {
    val suffix = if(forUpdate) " FOR UPDATE" else ""
    using(conn.prepareStatement("SELECT lifecycle_version, primary_key FROM version_map WHERE dataset_system_id = ? AND lifecycle_stage = CAST(? AS dataset_lifecycle_stage) ORDER BY lifecycle_version DESC OFFSET ? LIMIT 1" + suffix)) { stmt =>
      stmt.setLong(1, table.systemId)
      stmt.setString(2, stage.name)
      stmt.setInt(3, nth)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          Some(VersionInfo(table, rs.getLong("lifecycle_version"), stage, Option(rs.getString("primary_key"))))
        } else {
          None
        }
      }
    }
  }

  def schema(versionInfo: VersionInfo) =
    using(conn.prepareStatement("SELECT logical_column, type_name, physical_column_base FROM column_map WHERE dataset_system_id = ? AND lifecycle_version = ?")) { stmt =>
      stmt.setLong(1, versionInfo.tableInfo.systemId)
      stmt.setLong(2, versionInfo.lifecycleVersion)
      using(stmt.executeQuery()) { rs =>
        val result = Map.newBuilder[String, ColumnInfo]
        while(rs.next()) {
          val col = rs.getString("logical_column")
          result += col -> ColumnInfo(versionInfo, col, rs.getString("type_name"), rs.getString("physical_column_base"))
        }
        result.result()
      }
    }

  def addColumn(versionInfo: VersionInfo, logicalName: String, typeName: String, physicalColumnBase: String): ColumnInfo =
    using(conn.prepareStatement("INSERT INTO column_map (dataset_system_id, lifecycle_version, logical_column, type_name, physical_column_base) VALUES (?, ?, ?, ?, ?)")) { stmt =>
      stmt.setLong(1, versionInfo.tableInfo.systemId)
      stmt.setLong(2, versionInfo.lifecycleVersion)
      stmt.setString(3, logicalName)
      stmt.setString(4, typeName)
      stmt.setString(5, physicalColumnBase)
      val count = stmt.executeUpdate()
      assert(count == 1, "Insert into column_map didn't create a row?")
      ColumnInfo(versionInfo, logicalName, typeName, physicalColumnBase)
    }

  def dropColumn(versionInfo: VersionInfo, logicalName: String) {
    using(conn.prepareStatement("DELETE FROM column_map WHERE dataset_system_id = ? AND lifecycle_version = ? AND logical_column = ?")) { stmt =>
      stmt.setLong(1, versionInfo.tableInfo.systemId)
      stmt.setLong(2, versionInfo.lifecycleVersion)
      stmt.setString(3, logicalName)
      val count = stmt.executeUpdate()
      assert(count == 1, "Delete from column_map didn't remove a row?")
    }
  }

  def renameColumn(versionInfo: VersionInfo, oldLogicalName: String, newLogicalName: String) {
    using(conn.prepareStatement("UPDATE column_map SET logical_column = ? WHERE dataset_system_id = ? AND lifecycle_version = ? AND logical_column = ?")) { stmt =>
      stmt.setString(1, newLogicalName)
      stmt.setLong(2, versionInfo.tableInfo.systemId)
      stmt.setLong(3, versionInfo.lifecycleVersion)
      stmt.setString(4, oldLogicalName)
      val count = stmt.executeUpdate()
      assert(count == 1, "Rename column in column_map didn't change a row?")
    }
  }

  def convertColumn(versionInfo: VersionInfo, logicalName: String, newType: String, newPhysicalColumnBase: String) {
    using(conn.prepareStatement("UPDATE column_map SET type_name = ?, physical_column_base = ? WHERE dataset_system_id = ? AND lifecycle_version = ? AND logical_column = ?")) { stmt =>
      stmt.setString(1, newType)
      stmt.setString(2, newPhysicalColumnBase)
      stmt.setLong(3, versionInfo.tableInfo.systemId)
      stmt.setLong(4, versionInfo.lifecycleVersion)
      stmt.setString(5, logicalName)
      val count = stmt.executeUpdate()
      assert(count == 1, "Change type of column in column_map didn't change a row?")
    }
  }

  def setUserPrimaryKey(versionInfo: VersionInfo, userPrimaryKey: Option[String]) {
    using(conn.prepareStatement("UPDATE version_map SET primary_key = ? WHERE dataset_system_id = ? AND lifecycle_version = ?")) { stmt =>
      userPrimaryKey match {
        case Some(pk) => stmt.setString(1, pk)
        case None => stmt.setNull(1, Types.VARCHAR)
      }
      stmt.setLong(2, versionInfo.tableInfo.systemId)
      stmt.setLong(3, versionInfo.lifecycleVersion)
      val count = stmt.executeUpdate()
      assert(count == 1, "Change primary key in version_map didn't change a row?")
    }
  }

  def dropCopy(versionInfo: VersionInfo): Boolean = {
    if(versionInfo.lifecycleStage != LifecycleStage.Snapshotted && versionInfo.lifecycleStage != LifecycleStage.Unpublished) {
      throw new IllegalArgumentException("Can only drop a snapshot or an unpublished copy of a dataset.")
    }

    using(conn.prepareStatement("DELETE FROM column_map WHERE dataset_system_id = ? AND lifecycle_version = ?")) { stmt =>
      stmt.setLong(1, versionInfo.tableInfo.systemId)
      stmt.setLong(2, versionInfo.lifecycleVersion)
      stmt.executeUpdate()
    }
    using(conn.prepareStatement("DELETE FROM version_map WHERE dataset_system_id = ? AND lifecycle_version = ? AND lifecycle_stage = CAST(? AS dataset_lifecycle_stage)")) { stmt =>
      stmt.setLong(1, versionInfo.tableInfo.systemId)
      stmt.setLong(2, versionInfo.lifecycleVersion)
      stmt.setString(3, versionInfo.lifecycleStage.name) // just to make sure the user wasn't lying about the stage
      stmt.executeUpdate() == 1
    }
  }

  def ensureUnpublishedCopy(tableInfo: TableInfo): VersionInfo = {
    lockTableRow(tableInfo)
    unpublished(tableInfo) match {
      case Some(unpublished) => unpublished
      case None =>
        published(tableInfo) match {
          case Some(publishedCopy) =>
            val newLifecycleVersion = using(conn.prepareStatement("SELECT max(lifecycle_version) + 1 FROM version_map WHERE dataset_system_id = ?")) { stmt =>
              stmt.setLong(1, publishedCopy.tableInfo.systemId)
              using(stmt.executeQuery()) { rs =>
                rs.next()
                rs.getLong(1)
              }
            }

            val newVersion = publishedCopy.copy(
              lifecycleVersion = newLifecycleVersion,
              lifecycleStage = LifecycleStage.Unpublished)

            using(conn.prepareStatement("INSERT INTO version_map (dataset_system_id, lifecycle_version, lifecycle_stage, primary_key) values (?, ?, CAST(? AS dataset_lifecycle_stage), ?)")) { stmt =>
              stmt.setLong(1, newVersion.tableInfo.systemId)
              stmt.setLong(2, newVersion.lifecycleVersion)
              stmt.setString(3, newVersion.lifecycleStage.name)
              newVersion.userPrimaryKey match {
                case Some(pk) => stmt.setString(4, pk)
                case None => stmt.setNull(4, Types.VARCHAR)
              }
              val count = stmt.executeUpdate()
              assert(count == 1, "Making an unpublished copy didn't change a row?")
            }

            using(conn.prepareStatement("INSERT INTO column_map (dataset_system_id, lifecycle_version, logical_column, type_name, physical_column_base) SELECT dataset_system_id, ?, logical_column, type_name, physical_column_base FROM column_map WHERE lifecycle_version = ?")) { stmt =>
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

  def publish(tableInfo: TableInfo): Option[VersionInfo] = {
    lookup(tableInfo, LifecycleStage.Unpublished, forUpdate = true) map { workingCopy =>
      using(conn.prepareStatement("UPDATE version_map SET lifecycle_stage = CAST(? AS dataset_lifecycle_stage) WHERE dataset_system_id = ? AND lifecycle_version = ?")) { stmt =>
        for(published <- published(tableInfo)) {
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
