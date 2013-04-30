package com.socrata.datacoordinator
package truth.metadata
package sql

import scala.collection.immutable.VectorBuilder

import java.sql.{Connection, ResultSet, SQLException, Statement}

import org.postgresql.util.PSQLException
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.{DatasetSystemIdInUseByWriterException, DatasetIdInUseByWriterException}
import com.socrata.datacoordinator.id._
import com.socrata.datacoordinator.util.{TimingReport, PostgresUniqueViolation}
import com.socrata.datacoordinator.util.collection.MutableColumnIdMap
import com.socrata.datacoordinator.truth.metadata.CopyPair
import com.socrata.datacoordinator.truth.metadata.`-impl`._
import scala.concurrent.duration.Duration
import com.socrata.soql.environment.{ColumnName, TypeName}
import com.socrata.datacoordinator.truth.metadata.DatasetInfo
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.metadata.CopyInfo

trait BasePostgresDatasetMapReader[CT] extends `-impl`.BaseDatasetMapReader[CT] {
  implicit def typeNamespace: TypeNamespace[CT]
  implicit def tag: Tag = null

  val conn: Connection
  def t: TimingReport

  def snapshotCountQuery = "SELECT count(system_id) FROM copy_map WHERE dataset_system_id = ? AND lifecycle_stage = CAST(? AS dataset_lifecycle_stage)"
  def snapshotCount(dataset: DatasetInfo) =
    using(conn.prepareStatement(snapshotCountQuery)) { stmt =>
      stmt.setLong(1, dataset.systemId.underlying)
      stmt.setString(2, LifecycleStage.Snapshotted.name)
      using(t("shapshot-count", "dataset_id" -> dataset.systemId)(stmt.executeQuery())) { rs =>
        rs.next()
        rs.getInt(1)
      }
    }

  def latestQuery = "SELECT system_id, copy_number, lifecycle_stage :: TEXT, data_version FROM copy_map WHERE dataset_system_id = ? AND lifecycle_stage <> 'Discarded' ORDER BY copy_number DESC LIMIT 1"
  def latest(datasetInfo: DatasetInfo) =
    using(conn.prepareStatement(latestQuery)) { stmt =>
      stmt.setLong(1, datasetInfo.systemId.underlying)
      using(t("latest-copy", "dataset_id" -> datasetInfo.systemId)(stmt.executeQuery())) { rs =>
        if(!rs.next()) sys.error("Looked up a table for " + datasetInfo.datasetName + " but didn't find any copy info?")
        CopyInfo(
          datasetInfo,
          new CopyId(rs.getLong("system_id")),
          rs.getLong("copy_number"),
          LifecycleStage.valueOf(rs.getString("lifecycle_stage")),
          rs.getLong("data_version")
        )
      }
    }

  def allCopiesQuery = "SELECT system_id, copy_number, lifecycle_stage :: TEXT, data_version FROM copy_map WHERE dataset_system_id = ? ORDER BY copy_number"
  def allCopies(datasetInfo: DatasetInfo): Vector[CopyInfo] =
    using(conn.prepareStatement(allCopiesQuery)) { stmt =>
      stmt.setLong(1, datasetInfo.systemId.underlying)
      using(t("all-copies", "dataset_id" -> datasetInfo.systemId)(stmt.executeQuery()))(readCopies(datasetInfo))
    }

  private def readCopies(datasetInfo: DatasetInfo)(rs: ResultSet): Vector[CopyInfo] = {
    val result = new VectorBuilder[CopyInfo]
    while(rs.next()) {
      result += CopyInfo(
        datasetInfo,
        new CopyId(rs.getLong("system_id")),
        rs.getLong("copy_number"),
        LifecycleStage.valueOf(rs.getString("lifecycle_stage")),
        rs.getLong("data_version")
      )
    }
    result.result()
  }

  def lookupQuery = "SELECT system_id, copy_number, data_version FROM copy_map WHERE dataset_system_id = ? AND lifecycle_stage = CAST(? AS dataset_lifecycle_stage) ORDER BY copy_number DESC OFFSET ? LIMIT 1"
  def lookup(datasetInfo: DatasetInfo, stage: LifecycleStage, nth: Int = 0): Option[CopyInfo] = {
    using(conn.prepareStatement(lookupQuery)) { stmt =>
      stmt.setLong(1, datasetInfo.systemId.underlying)
      stmt.setString(2, stage.name)
      stmt.setInt(3, nth)
      using(t("lookup-copy","dataset_id" -> datasetInfo.systemId,"lifecycle-stage"->stage,"n" -> nth)(stmt.executeQuery())) { rs =>
        if(rs.next()) {
          Some(CopyInfo(datasetInfo, new CopyId(rs.getLong("system_id")), rs.getLong("copy_number"), stage, rs.getLong("data_version")))
        } else {
          None
        }
      }
    }
  }

  def previousVersionQuery = "SELECT system_id, copy_number, lifecycle_stage :: TEXT, data_version FROM copy_map WHERE dataset_system_id = ? AND copy_number < ? AND lifecycle_stage <> 'Discarded' ORDER BY copy_number DESC LIMIT 1"
  def previousVersion(copyInfo: CopyInfo): Option[CopyInfo] = {
    using(conn.prepareStatement(previousVersionQuery)) { stmt =>
      stmt.setLong(1, copyInfo.datasetInfo.systemId.underlying)
      stmt.setLong(2, copyInfo.copyNumber)
      using(t("previous-version","dataset_id" -> copyInfo.datasetInfo.systemId,"copy_num" -> copyInfo.copyNumber)(stmt.executeQuery())) { rs =>
        if(rs.next()) {
          Some(CopyInfo(
            copyInfo.datasetInfo,
            new CopyId(rs.getLong("system_id")),
            rs.getLong("copy_number"),
            LifecycleStage.valueOf(rs.getString("lifecycle_stage")),
            rs.getLong("data_version")))
        } else {
          None
        }
      }
    }
  }

  def copyNumberQuery = "SELECT system_id, lifecycle_stage, data_version FROM copy_map WHERE dataset_system_id = ? AND copy_number = ?"
  def copyNumber(datasetInfo: DatasetInfo, copyNumber: Long): Option[CopyInfo] =
    using(conn.prepareStatement(copyNumberQuery)) { stmt =>
      stmt.setLong(1, datasetInfo.systemId.underlying)
      stmt.setLong(2, copyNumber)
      using(t("copy-by-number", "dataset_id" -> datasetInfo.systemId, "copy_num" -> copyNumber)(stmt.executeQuery())) { rs =>
        if(rs.next()) {
          Some(CopyInfo(
            datasetInfo,
            new CopyId(rs.getLong("system_id")),
            copyNumber,
            LifecycleStage.valueOf(rs.getString("lifecycle_stage")),
            rs.getLong("data_version")
          ))
        } else {
          None
        }
      }
    }

  def schemaQuery = "SELECT system_id, logical_column_orig, logical_column_folded, type_name, physical_column_base_base, (is_system_primary_key IS NOT NULL) is_system_primary_key, (is_user_primary_key IS NOT NULL) is_user_primary_key FROM column_map WHERE copy_system_id = ?"
  def schema(copyInfo: CopyInfo) = {
    using(conn.prepareStatement(schemaQuery)) { stmt =>
      stmt.setLong(1, copyInfo.systemId.underlying)
      using(t("schema-lookup","dataset_id"->copyInfo.datasetInfo.systemId,"copy_num"->copyInfo.copyNumber)(stmt.executeQuery())) { rs =>
        val result = new MutableColumnIdMap[ColumnInfo[CT]]
        while(rs.next()) {
          val systemId = new ColumnId(rs.getLong("system_id"))
          val columnName = ColumnName(rs.getString("logical_column_orig"))
          assert(columnName.caseFolded == rs.getString("logical_column_folded"), "logical_column_orig and logical_column_folded have gotten out of sync!  Copy (sysid): " + copyInfo.systemId + "; id: " + systemId)
          result += systemId -> ColumnInfo(
            copyInfo,
            systemId,
            columnName,
            typeNamespace.typeForName(copyInfo.datasetInfo, rs.getString("type_name")),
            rs.getString("physical_column_base_base"),
            rs.getBoolean("is_system_primary_key"),
            rs.getBoolean("is_user_primary_key"))
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

  def snapshotsQuery = "SELECT system_id, copy_number, lifecycle_stage :: TEXT, data_version FROM copy_map WHERE dataset_system_id = ? AND lifecycle_stage = CAST(? AS dataset_lifecycle_stage) ORDER BY copy_number"
  def snapshots(datasetInfo: DatasetInfo): Vector[CopyInfo] =
    using(conn.prepareStatement(snapshotsQuery)) { stmt =>
      stmt.setLong(1, datasetInfo.systemId.underlying)
      stmt.setString(2, LifecycleStage.Snapshotted.name)
      using(t("snapshots", "dataset_id" -> datasetInfo.systemId)(stmt.executeQuery()))(readCopies(datasetInfo))
    }

  def datasetIdByUserIdQuery = "SELECT system_id FROM dataset_map WHERE dataset_name = ?"
  def datasetId(datasetId: String) =
    using(conn.prepareStatement(datasetIdByUserIdQuery)) { stmt =>
      stmt.setString(1, datasetId)
      using(t("dataset-id-by-name","name"->datasetId)(stmt.executeQuery())) { rs =>
        if(rs.next()) Some(new DatasetId(rs.getLong(1)))
        else None
      }
    }
}

class PostgresDatasetMapReader[CT](val conn: Connection, tns: TypeNamespace[CT], timingReport: TimingReport) extends DatasetMapReader[CT] with BasePostgresDatasetMapReader[CT] {
  implicit def typeNamespace = tns
  def t = timingReport

  def datasetInfoBySystemIdQuery = "SELECT system_id, dataset_name, table_base_base, next_counter_value, locale_name, obfuscation_key FROM dataset_map WHERE system_id = ?"
  def datasetInfo(datasetId: DatasetId) =
    using(conn.prepareStatement(datasetInfoBySystemIdQuery)) { stmt =>
      stmt.setLong(1, datasetId.underlying)
      using(t("lookup-dataset", "dataset_id" -> datasetId)(stmt.executeQuery())) { rs =>
        if(rs.next()) {
          Some(DatasetInfo(new DatasetId(rs.getLong("system_id")), rs.getString("dataset_name"), rs.getString("table_base_base"), rs.getLong("next_counter_value"), rs.getString("locale_name"), rs.getBytes("obfuscation_key")))
        } else {
          None
        }
      }
    }
}

trait BasePostgresDatasetMapWriter[CT] extends BasePostgresDatasetMapReader[CT] with `-impl`.BaseDatasetMapWriter[CT] {
  val obfuscationKeyGenerator: () => Array[Byte]
  val initialCounterValue: Long

  def createQuery_tableMap = "INSERT INTO dataset_map (dataset_name, table_base_base, next_counter_value, locale_name, obfuscation_key) VALUES (?, ?, ?, ?, ?) RETURNING system_id"
  def createQuery_copyMap = "INSERT INTO copy_map (dataset_system_id, copy_number, lifecycle_stage, data_version) VALUES (?, ?, CAST(? AS dataset_lifecycle_stage), ?) RETURNING system_id"
  def create(datasetId: String, tableBaseBase: String, localeName: String): CopyInfo = {
    val datasetInfo = using(conn.prepareStatement(createQuery_tableMap)) { stmt =>
      val datasetInfoNoSystemId = DatasetInfo(new DatasetId(-1), datasetId, tableBaseBase, initialCounterValue, localeName, obfuscationKeyGenerator())
      stmt.setString(1, datasetInfoNoSystemId.datasetName)
      stmt.setString(2, datasetInfoNoSystemId.tableBaseBase)
      stmt.setLong(3, datasetInfoNoSystemId.nextCounterValue)
      stmt.setString(4, datasetInfoNoSystemId.localeName)
      stmt.setBytes(5, datasetInfoNoSystemId.obfuscationKey)
      try {
        using(t("create-dataset", "dataset_name" -> datasetId)(stmt.executeQuery())) { rs =>
          val foundSomething = rs.next()
          assert(foundSomething, "INSERT didn't return a system id?")
          datasetInfoNoSystemId.copy(systemId = new DatasetId(rs.getLong(1)))
        }
      } catch {
        case PostgresUniqueViolation("dataset_name") =>
          throw new DatasetAlreadyExistsException(datasetId)
      }
    }

    using(conn.prepareStatement(createQuery_copyMap)) { stmt =>
      val copyInfoNoSystemId = CopyInfo(datasetInfo, new CopyId(-1), 1, LifecycleStage.Unpublished, 0)

      stmt.setLong(1, copyInfoNoSystemId.datasetInfo.systemId.underlying)
      stmt.setLong(2, copyInfoNoSystemId.copyNumber)
      stmt.setString(3, copyInfoNoSystemId.lifecycleStage.name)
      stmt.setLong(4, copyInfoNoSystemId.dataVersion)
      using(t("create-initial-copy", "dataset_id" -> datasetInfo.systemId)(stmt.executeQuery())) { rs =>
        val foundSomething = rs.next()
        assert(foundSomething, "Didn't return a system ID?")
        copyInfoNoSystemId.copy(systemId = new CopyId(rs.getLong(1)))
      }
    }
  }

  def createQuery_copyMapWithSystemId = "INSERT INTO copy_map (system_id, dataset_system_id, copy_number, lifecycle_stage, data_version) VALUES (?, ?, ?, CAST(? AS dataset_lifecycle_stage), ?)"
  def createWithId(systemId: DatasetId, datasetId: String, tableBaseBase: String, initialCopyId: CopyId, localeName: String, obfuscationKey: Array[Byte]): CopyInfo = {
    val datasetInfo = unsafeCreateDataset(systemId, datasetId, tableBaseBase, initialCounterValue, localeName, obfuscationKey)

    using(conn.prepareStatement(createQuery_copyMapWithSystemId)) { stmt =>
      val copyInfo = CopyInfo(datasetInfo, initialCopyId, 1, LifecycleStage.Unpublished, 0)

      stmt.setLong(1, copyInfo.systemId.underlying)
      stmt.setLong(2, copyInfo.datasetInfo.systemId.underlying)
      stmt.setLong(3, copyInfo.copyNumber)
      stmt.setString(4, copyInfo.lifecycleStage.name)
      stmt.setLong(5, copyInfo.dataVersion)
      try {
        t("create-create-copy-with-system-id", "dataset_id" -> systemId, "copy_id" -> initialCopyId)(stmt.execute())
      } catch {
        case PostgresUniqueViolation("system_id") =>
          throw new CopySystemIdAlreadyInUse(initialCopyId)
      }

      copyInfo
    }
  }

  // Yay no "DELETE ... CASCADE"!
  def deleteQuery_columnMap = "DELETE FROM column_map WHERE copy_system_id IN (SELECT system_id FROM copy_map WHERE dataset_system_id = ?)"
  def deleteQuery_copyMap = "DELETE FROM copy_map WHERE dataset_system_id = ?"
  def deleteQuery_tableMap = "DELETE FROM dataset_map WHERE system_id = ?"
  def delete(tableInfo: DatasetInfo) {
    deleteCopiesOf(tableInfo)
    using(conn.prepareStatement(deleteQuery_tableMap)) { stmt =>
      stmt.setLong(1, tableInfo.systemId.underlying)
      val count = t("delete-dataset", "dataset_id" -> tableInfo.systemId)(stmt.executeUpdate())
      assert(count == 1, "Called delete on a table which is no longer there?")
    }
  }

  def deleteCopiesOf(datasetInfo: DatasetInfo) {
    using(conn.prepareStatement(deleteQuery_columnMap)) { stmt =>
      stmt.setLong(1, datasetInfo.systemId.underlying)
      t("delete-dataset-columns", "dataset_id" -> datasetInfo.systemId)(stmt.executeUpdate())
    }
    using(conn.prepareStatement(deleteQuery_copyMap)) { stmt =>
      stmt.setLong(1, datasetInfo.systemId.underlying)
      t("delete-dataset-copies", "dataset_id" -> datasetInfo.systemId)(stmt.executeUpdate())
    }
  }

  // like file descriptors, new columns always get the smallest available ID.  But "smallest available"
  // means "not used by this version OR THE PREVIOUS VERSION" so we can track column identity across
  // publication cycles.
  def findFirstFreeColumnIdQuery =
    """-- Adapted from http://johtopg.blogspot.com/2010/07/smallest-available-id.html
      |-- Use zero if available
      |(SELECT
      |    0 AS next_system_id
      | WHERE
      |    NOT EXISTS
      |        (SELECT 1 FROM column_map WHERE system_id = 0 AND (copy_system_id = ? OR copy_system_id = ?)) )
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
      |        (copy_system_id = ? OR copy_system_id = ?)
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
  def findFirstFreeColumnId(copyInfoA: CopyInfo, copyInfoB: CopyInfo): ColumnId = {
    using(conn.prepareStatement(findFirstFreeColumnIdQuery)) { stmt =>
      stmt.setLong(1, copyInfoA.systemId.underlying)
      stmt.setLong(2, copyInfoB.systemId.underlying)
      stmt.setLong(3, copyInfoA.systemId.underlying)
      stmt.setLong(4, copyInfoB.systemId.underlying)
      using(t("find-first-free-column-id", "dataset_id" -> copyInfoA.datasetInfo.systemId, "copy_num_a" -> copyInfoA.copyNumber, "copy_num_b" -> copyInfoB.copyNumber)(stmt.executeQuery())) { rs =>
        val foundSomething = rs.next()
        assert(foundSomething, "Finding the last column info didn't return anything?")
        new ColumnId(rs.getLong("next_system_id"))
      }
    }
  }

  def addColumn(copyInfo: CopyInfo, logicalName: ColumnName, typ: CT, physicalColumnBaseBase: String): ColumnInfo[CT] = {
    val systemId =
      previousVersion(copyInfo) match {
        case Some(previousCopy) =>
          findFirstFreeColumnId(copyInfo, previousCopy)
        case None =>
          findFirstFreeColumnId(copyInfo, copyInfo)
      }

    addColumnWithId(systemId, copyInfo, logicalName, typ, physicalColumnBaseBase)
  }

  def addColumnQuery = "INSERT INTO column_map (system_id, copy_system_id, logical_column_orig, logical_column_folded, type_name, physical_column_base_base) VALUES (?, ?, ?, ?, ?, ?)"
  def addColumnWithId(systemId: ColumnId, copyInfo: CopyInfo, logicalName: ColumnName, typ: CT, physicalColumnBaseBase: String): ColumnInfo[CT] = {
    using(conn.prepareStatement(addColumnQuery)) { stmt =>
      val columnInfo = ColumnInfo[CT](copyInfo, systemId, logicalName, typ, physicalColumnBaseBase, isSystemPrimaryKey = false, isUserPrimaryKey = false)

      stmt.setLong(1, columnInfo.systemId.underlying)
      stmt.setLong(2, columnInfo.copyInfo.systemId.underlying)
      stmt.setString(3, logicalName.name)
      stmt.setString(4, logicalName.caseFolded)
      stmt.setString(5, typeNamespace.nameForType(typ))
      stmt.setString(6, physicalColumnBaseBase)
      try {
        t("add-column-with-id", "dataset_id" -> copyInfo.datasetInfo.systemId, "copy_num" -> copyInfo.copyNumber, "column_id" -> systemId)(stmt.execute())
      } catch {
        case PostgresUniqueViolation("copy_system_id", "system_id") =>
          throw new ColumnSystemIdAlreadyInUse(copyInfo, systemId)
        case PostgresUniqueViolation("copy_system_id", "logical_column_folded") =>
          throw new ColumnAlreadyExistsException(copyInfo, logicalName)
      }

      columnInfo
    }
  }

  def unsafeCreateDatasetQuery = "INSERT INTO dataset_map (system_id, dataset_name, table_base_base, next_counter_value, locale_name, obfuscation_key) VALUES (?, ?, ?, ?, ?, ?)"
  def unsafeCreateDataset(systemId: DatasetId, datasetId: String, tableBaseBase: String, nextCounterValue: Long, localeName: String, obfuscationKey: Array[Byte]): DatasetInfo = {
    val datasetInfo = DatasetInfo(systemId, datasetId, tableBaseBase, nextCounterValue, localeName, obfuscationKey)

    using(conn.prepareStatement(unsafeCreateDatasetQuery)) { stmt =>
      stmt.setLong(1, datasetInfo.systemId.underlying)
      stmt.setString(2, datasetInfo.datasetName)
      stmt.setString(3, datasetInfo.tableBaseBase)
      stmt.setLong(4, datasetInfo.nextCounterValue)
      stmt.setString(5, datasetInfo.localeName)
      stmt.setBytes(6, datasetInfo.obfuscationKey)
      try {
        t("unsafe-create-dataset", "dataset_id" -> systemId)(stmt.execute())
        stmt.execute()
      } catch {
        case PostgresUniqueViolation("system_id") =>
          throw new DatasetSystemIdAlreadyInUse(systemId)
        case PostgresUniqueViolation("dataset_name") =>
          throw new DatasetAlreadyExistsException(datasetId)
      }
    }

    datasetInfo
  }

  val unsafeReloadDatasetQuery = "UPDATE dataset_map SET dataset_name = ?, table_base_base = ?, next_counter_value = ?, locale_name = ?, obfuscation_key = ? WHERE system_id = ?"
  def unsafeReloadDataset(datasetInfo: DatasetInfo,
                          datasetId: String,
                          tableBaseBase: String,
                          nextCounterValue: Long,
                          localeName: String,
                          obfuscationKey: Array[Byte]): DatasetInfo = {
    val newDatasetInfo = DatasetInfo(datasetInfo.systemId, datasetId, tableBaseBase, nextCounterValue, localeName, obfuscationKey)
    using(conn.prepareStatement(unsafeReloadDatasetQuery)) { stmt =>
      stmt.setString(1, newDatasetInfo.datasetName)
      stmt.setString(2, newDatasetInfo.tableBaseBase)
      stmt.setLong(3, newDatasetInfo.nextCounterValue)
      stmt.setLong(4, newDatasetInfo.systemId.underlying)
      stmt.setString(5, newDatasetInfo.localeName)
      stmt.setBytes(6, newDatasetInfo.obfuscationKey)
      try {
        val updated = t("unsafe-reload-dataset", "dataset_id" -> datasetInfo.systemId)(stmt.executeUpdate())
        assert(updated == 1, s"Dataset ${datasetInfo.systemId.underlying} does not exist?")
      } catch {
        case PostgresUniqueViolation("dataset_name") =>
          throw new DatasetAlreadyExistsException(datasetId)
      }
    }

    deleteCopiesOf(newDatasetInfo)

    newDatasetInfo
  }

  def unsafeCreateCopyQuery = "INSERT INTO copy_map (system_id, dataset_system_id, copy_number, lifecycle_stage, data_version) values (?, ?, ?, CAST(? AS dataset_lifecycle_stage), ?)"
  def unsafeCreateCopy(datasetInfo: DatasetInfo,
                       systemId: CopyId,
                       copyNumber: Long,
                       lifecycleStage: LifecycleStage,
                       dataVersion: Long): CopyInfo = {
    val newCopy = CopyInfo(datasetInfo, systemId, copyNumber, lifecycleStage, dataVersion)

    using(conn.prepareStatement(unsafeCreateCopyQuery)) { stmt =>
      stmt.setLong(1, newCopy.systemId.underlying)
      stmt.setLong(2, newCopy.datasetInfo.systemId.underlying)
      stmt.setLong(3, newCopy.copyNumber)
      stmt.setString(4, newCopy.lifecycleStage.name)
      stmt.setLong(5, newCopy.dataVersion)
      try {
        t("unsafe-create-copy", "dataset_id" -> datasetInfo.systemId, "copy_num" -> copyNumber)(stmt.execute())
      } catch {
        case PostgresUniqueViolation("system_id") =>
          throw new CopySystemIdAlreadyInUse(systemId)
      }
    }

    newCopy
  }

  def dropColumnQuery = "DELETE FROM column_map WHERE copy_system_id = ? AND system_id = ?"
  def dropColumn(columnInfo: ColumnInfo[CT]) {
    using(conn.prepareStatement(dropColumnQuery)) { stmt =>
      stmt.setLong(1, columnInfo.copyInfo.systemId.underlying)
      stmt.setLong(2, columnInfo.systemId.underlying)
      val count = t("drop-column", "dataset_id" -> columnInfo.copyInfo.datasetInfo.systemId, "copy_num" -> columnInfo.copyInfo.copyNumber, "column_id" -> columnInfo.systemId)(stmt.executeUpdate())
      assert(count == 1, "Column did not exist to be dropped?")
    }
  }

  def renameColumnQuery = "UPDATE column_map SET logical_column_orig = ?, logical_column_folded = ? WHERE copy_system_id = ? AND system_id = ?"
  def renameColumn(columnInfo: ColumnInfo[CT], newLogicalName: ColumnName): ColumnInfo[CT] =
    using(conn.prepareStatement(renameColumnQuery)) { stmt =>
      stmt.setString(1, newLogicalName.name)
      stmt.setString(2, newLogicalName.caseFolded)
      stmt.setLong(3, columnInfo.copyInfo.systemId.underlying)
      stmt.setLong(4, columnInfo.systemId.underlying)
      val count = t("rename-column", "dataset_id" -> columnInfo.copyInfo.datasetInfo.systemId, "copy_num" -> columnInfo.copyInfo.copyNumber, "column_id" -> columnInfo.systemId)(stmt.executeUpdate())
      assert(count == 1, "Column did not exist to be renamed?")
      columnInfo.copy(logicalName = newLogicalName)
    }

  def convertColumnQuery = "UPDATE column_map SET type_name = ?, physical_column_base_base = ? WHERE copy_system_id = ? AND system_id = ?"
  def convertColumn(columnInfo: ColumnInfo[CT], newType: CT, newPhysicalColumnBaseBase: String): ColumnInfo[CT] =
    using(conn.prepareStatement(convertColumnQuery)) { stmt =>
      stmt.setString(1, typeNamespace.nameForType(newType))
      stmt.setString(2, newPhysicalColumnBaseBase)
      stmt.setLong(3, columnInfo.copyInfo.systemId.underlying)
      stmt.setLong(4, columnInfo.systemId.underlying)
      val count = t("convert-column", "dataset_id" -> columnInfo.copyInfo.datasetInfo.systemId, "copy_num" -> columnInfo.copyInfo.copyNumber, "column_id" -> columnInfo.systemId)(stmt.executeUpdate())
      assert(count == 1, "Column did not exist to be converted?")
      columnInfo.copy(typ = newType, physicalColumnBaseBase = newPhysicalColumnBaseBase)
    }

  def setSystemPrimaryKeyQuery = "UPDATE column_map SET is_system_primary_key = 'Unit' WHERE copy_system_id = ? AND system_id = ?"
  def setSystemPrimaryKey(columnInfo: ColumnInfo[CT]) =
    using(conn.prepareStatement(setSystemPrimaryKeyQuery)) { stmt =>
      stmt.setLong(1, columnInfo.copyInfo.systemId.underlying)
      stmt.setLong(2, columnInfo.systemId.underlying)
      val count = t("set-system-primary-key", "dataset_id" -> columnInfo.copyInfo.datasetInfo.systemId, "copy_num" -> columnInfo.copyInfo.copyNumber, "column_id" -> columnInfo.systemId)(stmt.executeUpdate())
      assert(count == 1, "Column did not exist to have it set as primary key?")
      columnInfo.copy(isSystemPrimaryKey = true)
    }

  def setUserPrimaryKeyQuery = "UPDATE column_map SET is_user_primary_key = 'Unit' WHERE copy_system_id = ? AND system_id = ?"
  def setUserPrimaryKey(columnInfo: ColumnInfo[CT]) =
    using(conn.prepareStatement(setUserPrimaryKeyQuery)) { stmt =>
      stmt.setLong(1, columnInfo.copyInfo.systemId.underlying)
      stmt.setLong(2, columnInfo.systemId.underlying)
      val count = t("set-user-primary-key", "dataset_id" -> columnInfo.copyInfo.datasetInfo.systemId, "copy_num" -> columnInfo.copyInfo.copyNumber, "column_id" -> columnInfo.systemId)(stmt.executeUpdate())
      assert(count == 1, "Column did not exist to have it set as primary key?")
      columnInfo.copy(isUserPrimaryKey = true)
    }

  def clearUserPrimaryKeyQuery = "UPDATE column_map SET is_user_primary_key = NULL WHERE copy_system_id = ? and system_id = ?"
  def clearUserPrimaryKey(columnInfo: ColumnInfo[CT]) = {
    require(columnInfo.isUserPrimaryKey, "Requested clearing a non-primary key")
    using(conn.prepareStatement(clearUserPrimaryKeyQuery)) { stmt =>
      stmt.setLong(1, columnInfo.copyInfo.systemId.underlying)
      stmt.setLong(2, columnInfo.systemId.underlying)
      stmt.executeUpdate()
    }
    columnInfo.copy(isUserPrimaryKey = false)
  }

  def updateNextCounterValueQuery = "UPDATE dataset_map SET next_counter_value = ? WHERE system_id = ?"
  def updateNextCounterValue(datasetInfo: DatasetInfo, newNextCounterValue: Long): DatasetInfo = {
    assert(newNextCounterValue >= datasetInfo.nextCounterValue)
    if(newNextCounterValue != datasetInfo.nextCounterValue) {
      using(conn.prepareStatement(updateNextCounterValueQuery)) { stmt =>
        stmt.setLong(1, newNextCounterValue)
        stmt.setLong(2, datasetInfo.systemId.underlying)
        t("update-next-row-id", "dataset_id" -> datasetInfo.systemId)(stmt.executeUpdate())
      }
      datasetInfo.copy(nextCounterValue = newNextCounterValue)
    } else {
      datasetInfo
    }
  }

  def updateNextCounterValue(copyInfo: CopyInfo, newNextCounterValue: Long): CopyInfo =
    copyInfo.copy(datasetInfo = updateNextCounterValue(copyInfo.datasetInfo, newNextCounterValue))

  def updateDataVersionQuery = "UPDATE copy_map SET data_version = ? WHERE system_id = ?"
  def updateDataVersion(copyInfo: CopyInfo, newDataVersion: Long): CopyInfo = {
    // Not "== copyInfo.dataVersion + 1" because if a working copy was dropped
    assert(newDataVersion > copyInfo.dataVersion, s"Setting data version to $newDataVersion when it was ${copyInfo.dataVersion}")
    using(conn.prepareStatement(updateDataVersionQuery)) { stmt =>
      stmt.setLong(1, newDataVersion)
      stmt.setLong(2, copyInfo.systemId.underlying)
      val count = t("update-data-version", "dataset_id" -> copyInfo.datasetInfo.systemId, "copy_num" -> copyInfo.copyNumber)(stmt.executeUpdate())
      assert(count == 1)
    }
    copyInfo.copy(dataVersion = newDataVersion)
  }

  def dropCopyQuery = "UPDATE copy_map SET lifecycle_stage = 'Discarded' WHERE system_id = ? AND lifecycle_stage = CAST(? AS dataset_lifecycle_stage)"
  def dropCopy(copyInfo: CopyInfo) {
    val validStages = Set(LifecycleStage.Snapshotted, LifecycleStage.Unpublished)
    if(!validStages(copyInfo.lifecycleStage)) {
      throw new CopyInWrongStateForDropException(copyInfo, validStages)
    }
    if(copyInfo.lifecycleStage == LifecycleStage.Unpublished && copyInfo.copyNumber == 1) {
      throw new CannotDropInitialWorkingCopyException(copyInfo)
    }

    using(conn.prepareStatement(dropCopyQuery)) { stmt =>
      stmt.setLong(1, copyInfo.systemId.underlying)
      stmt.setString(2, copyInfo.lifecycleStage.name) // just to make sure the user wasn't mistaken about the stage
      val count = t("drop-copy", "dataset_id" -> copyInfo.datasetInfo.systemId, "copy_num" -> copyInfo.copyNumber)(stmt.executeUpdate())
      assert(count == 1, "Copy did not exist to be dropped?")
    }
  }

  def ensureUnpublishedCopyQuery_newCopyNumber = "SELECT max(copy_number) + 1 FROM copy_map WHERE dataset_system_id = ?"
  def ensureUnpublishedCopyQuery_copyMap = "INSERT INTO copy_map (dataset_system_id, copy_number, lifecycle_stage, data_version) values (?, ?, CAST(? AS dataset_lifecycle_stage), ?) RETURNING system_id"
  def ensureUnpublishedCopy(tableInfo: DatasetInfo): Either[CopyInfo, CopyPair[CopyInfo]] =
    ensureUnpublishedCopy(tableInfo, None)

  def createUnpublishedCopyWithId(tableInfo: DatasetInfo, copyId: CopyId): CopyPair[CopyInfo] =
    ensureUnpublishedCopy(tableInfo, Some(copyId)).right.getOrElse {
      throw new CopySystemIdAlreadyInUse(copyId)
    }

  def ensureUnpublishedCopy(tableInfo: DatasetInfo, newCopyId: Option[CopyId]): Either[CopyInfo, CopyPair[CopyInfo]] =
    lookup(tableInfo, LifecycleStage.Unpublished) match {
      case Some(unpublished) =>
        Left(unpublished)
      case None =>
        lookup(tableInfo, LifecycleStage.Published) match {
          case Some(publishedCopy) =>
            val newCopyNumber = using(conn.prepareStatement(ensureUnpublishedCopyQuery_newCopyNumber)) { stmt =>
              stmt.setLong(1, publishedCopy.datasetInfo.systemId.underlying)
              using(t("find-next-copy-number","dataset_id" -> tableInfo.systemId)(stmt.executeQuery())) { rs =>
                rs.next()
                rs.getLong(1)
              }
            }

            val newCopy = newCopyId match {
              case None =>
                val newCopyWithoutSystemId = publishedCopy.copy(
                  systemId = new CopyId(-1),
                  copyNumber = newCopyNumber,
                  lifecycleStage = LifecycleStage.Unpublished)

                val newCopy = using(conn.prepareStatement(ensureUnpublishedCopyQuery_copyMap)) { stmt =>
                  stmt.setLong(1, newCopyWithoutSystemId.datasetInfo.systemId.underlying)
                  stmt.setLong(2, newCopyWithoutSystemId.copyNumber)
                  stmt.setString(3, newCopyWithoutSystemId.lifecycleStage.name)
                  stmt.setLong(4, newCopyWithoutSystemId.dataVersion)
                  using(t("create-new-copy", "dataset_id" -> newCopyWithoutSystemId.datasetInfo.systemId)(stmt.executeQuery())) { rs =>
                    val foundSomething = rs.next()
                    assert(foundSomething, "Insert didn't create a row?")
                    newCopyWithoutSystemId.copy(systemId = new CopyId(rs.getLong(1)))
                  }
                }

                copySchemaIntoUnpublishedCopy(publishedCopy, newCopy)

                newCopy
              case Some(cid) =>
                unsafeCreateCopy(
                  publishedCopy.datasetInfo,
                  cid,
                  newCopyNumber,
                  LifecycleStage.Unpublished,
                  publishedCopy.dataVersion)
            }

            Right(CopyPair(publishedCopy, newCopy))
          case None =>
            sys.error("No published copy available?")
        }
    }

  def ensureUnpublishedCopyQuery_columnMap = "INSERT INTO column_map (copy_system_id, system_id, logical_column_orig, logical_column_folded, type_name, physical_column_base_base, is_system_primary_key, is_user_primary_key) SELECT ?, system_id, logical_column_orig, logical_column_folded, type_name, physical_column_base_base, null, null FROM column_map WHERE copy_system_id = ?"
  def copySchemaIntoUnpublishedCopy(oldCopy: CopyInfo, newCopy: CopyInfo) {
    using(conn.prepareStatement(ensureUnpublishedCopyQuery_columnMap)) { stmt =>
      stmt.setLong(1, newCopy.systemId.underlying)
      stmt.setLong(2, oldCopy.systemId.underlying)
      t("copy-schema-to-unpublished-copy", "dataset_id" -> oldCopy.datasetInfo.systemId, "old_copy_num" -> oldCopy.copyNumber, "new_copy_num" -> newCopy.copyNumber)(stmt.execute())
    }
  }

  def publishQuery = "UPDATE copy_map SET lifecycle_stage = CAST(? AS dataset_lifecycle_stage) WHERE system_id = ?"
  def publish(unpublishedCopy: CopyInfo): CopyInfo = {
    if(unpublishedCopy.lifecycleStage != LifecycleStage.Unpublished) {
      throw new IllegalArgumentException("Input does not name an unpublished copy")
    }
    using(conn.prepareStatement(publishQuery)) { stmt =>
      for(published <- lookup(unpublishedCopy.datasetInfo, LifecycleStage.Published)) {
        stmt.setString(1, LifecycleStage.Snapshotted.name)
        stmt.setLong(2, published.systemId.underlying)
        val count = t("snapshotify-published-copy", "dataset_id" -> published.datasetInfo.systemId, "copy_num" -> published.copyNumber)(stmt.executeUpdate())
        assert(count == 1, "Snapshotting a published copy didn't change a row?")
      }
      stmt.setString(1, LifecycleStage.Published.name)
      stmt.setLong(2, unpublishedCopy.systemId.underlying)
      val count = t("publish-unpublished-copy", "dataset_id" -> unpublishedCopy.datasetInfo.systemId, "copy_num" -> unpublishedCopy.copyNumber)(stmt.executeUpdate())
      assert(count == 1, "Publishing an unpublished copy didn't change a row?")
      unpublishedCopy.copy(lifecycleStage = LifecycleStage.Published)
    }
  }
}

class PostgresDatasetMapWriter[CT](val conn: Connection, tns: TypeNamespace[CT], timingReport: TimingReport, val obfuscationKeyGenerator: () => Array[Byte], val initialCounterValue: Long) extends DatasetMapWriter[CT] with BasePostgresDatasetMapWriter[CT] with BackupDatasetMap[CT] {
  implicit def typeNamespace = tns
  require(!conn.getAutoCommit, "Connection is in auto-commit mode")

  import PostgresDatasetMapWriter._

  def t = timingReport

  def lockNotAvailableState = "55P03"
  def queryCancelledState = "57014"

  // Can't set parameters' values via prepared statement placeholders
  def setTimeout(timeoutMs: Int) =
    s"SET LOCAL statement_timeout TO $timeoutMs"
  def datasetInfoBySystemIdQuery =
    "SELECT system_id, dataset_name, table_base_base, next_counter_value, locale_name, obfuscation_key FROM dataset_map WHERE system_id = ? FOR UPDATE"
  def resetTimeout = "SET LOCAL statement_timeout TO DEFAULT"
  def datasetInfo(datasetId: DatasetId, timeout: Duration) = {
    // For now we assume that we're the only one setting the statement_timeout
    // parameter.  If this turns out to be wrong, we'll have to SHOW the
    // parameter in order to
    //   * set the value properly in the case of a nonfinite timeout
    //   * save the value to restore.
    // One might think this would be better done as a stored procedure,
    // which can do the save/restore thing automatically -- see the paragraph
    // that begins "If SET LOCAL is used within a function..." at
    //     http://www.postgresql.org/docs/9.2/static/sql-set.html
    // but unfortunately setting statement_timeout doesn't affect the
    // _current_ statement.  For this same reason we're not just doing all
    // three operations in a single "set timeout;query;restore timeout"
    // call.
    val savepoint = conn.setSavepoint()
    try {
      if(timeout.isFinite()) {
        val ms = timeout.toMillis.min(Int.MaxValue).max(1).toInt
        log.trace("Setting statement timeout to {}ms", ms)
        execute(setTimeout(ms))
      }
      val result =
        using(conn.prepareStatement(datasetInfoBySystemIdQuery)) { stmt =>
          stmt.setLong(1, datasetId.underlying)
          try {
            t("lookup-dataset-for-update", "dataset_id" -> datasetId)(stmt.execute())
            getInfoResult(stmt)
          } catch {
            case e: PSQLException if isStatementTimeout(e) =>
              log.trace("Get dataset _with_ waiting failed; abandoning")
              conn.rollback(savepoint)
              throw new DatasetSystemIdInUseByWriterException(datasetId, e)
          }
        }
      if(timeout.isFinite) execute(resetTimeout)
      result
    } finally {
      try {
        conn.releaseSavepoint(savepoint)
      } catch {
        case e: SQLException =>
          // Ignore; this means one of two things:
          // * the server is in an unexpected "transaction aborted" state, so all we
          //    can do is roll back (either to another, earlier savepoint or completely)
          //    and either way this savepoint will be dropped implicitly
          // * things have completely exploded and nothing can be done except
          //    dropping the connection altogether.
          // The latter could happen if this finally block is being run because
          // this method is exiting normally, but in that case whatever we do next
          // will fail so meh.  Just log it and continue.
          log.warn("Unexpected exception while releasing savepoint", e)
      }
    }
  }

  def execute(s: String) {
    using(conn.createStatement()) { stmt =>
      log.trace("Executing simple SQL {}", s)
      stmt.execute(s)
    }
  }

  def getInfoResult(stmt: Statement) =
    using(stmt.getResultSet) { rs =>
      if(rs.next()) {
        Some(DatasetInfo(new DatasetId(rs.getLong("system_id")), rs.getString("dataset_name"), rs.getString("table_base_base"), rs.getLong("next_counter_value"), rs.getString("locale_name"), rs.getBytes("obfuscation_key")))
      } else {
        None
      }
    }

  def isStatementTimeout(e: PSQLException): Boolean =
    e.getSQLState == queryCancelledState &&
      errorMessage(e).map(_.endsWith("due to statement timeout")).getOrElse(false)

  def errorMessage(e: PSQLException): Option[String] =
    for {
      serverErrorMessage <- Option(e.getServerErrorMessage)
      message <- Option(serverErrorMessage.getMessage)
    } yield message
}

object PostgresDatasetMapWriter {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[PostgresDatasetMapWriter[_]])
}
