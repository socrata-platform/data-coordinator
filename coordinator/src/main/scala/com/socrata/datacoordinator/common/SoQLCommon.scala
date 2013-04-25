package com.socrata.datacoordinator.common

import com.socrata.datacoordinator.{Row, MutableRow}
import com.socrata.datacoordinator.service.{SchemaFinder, MutatorCommon}
import com.socrata.soql.types.{SoQLFixedTimestamp, SoQLID, SoQLValue, SoQLType}
import com.socrata.soql.environment.{ColumnName, TypeName}
import com.socrata.soql.brita.{AsciiIdentifierFilter, IdentifierFilter}
import com.socrata.datacoordinator.common.soql.{SoQLRowLogCodec, SoQLRep, SoQLTypeContext}
import com.socrata.datacoordinator.common.util.RowIdObfuscator
import com.socrata.datacoordinator.truth.metadata.{AbstractColumnInfoLike, ColumnInfo}
import com.socrata.datacoordinator.truth.json.{JsonColumnRep, JsonColumnReadRep}
import java.util.concurrent.ExecutorService
import com.socrata.datacoordinator.truth.universe.sql.{PostgresUniverse, PostgresCommonSupport}
import org.joda.time.DateTime
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.loader.RowPreparer
import com.socrata.datacoordinator.id.RowId
import java.sql.Connection
import java.io.Reader
import com.socrata.datacoordinator.util.TransferrableContextTimingReport
import javax.sql.DataSource
import com.rojoma.simplearm.{SimpleArm, Managed}

class SoQLCommon(dataSource: DataSource,
                 copyInProvider: (Connection, String, Reader) => Long,
                 executorService: ExecutorService,
                 tableSpace: String => Option[String],
                 val timingReport: TransferrableContextTimingReport)
{ common =>
  type CT = SoQLType
  type CV = SoQLValue

  val datasetMapLimits = StandardDatasetMapLimits

  object SystemColumnNames {
    val id = ColumnName(":id")
    val createdAt = ColumnName(":created_at")
    val updatedAt = ColumnName(":updated_at")
  }

  val systemSchema = Map(
    SystemColumnNames.id -> SoQLID,
    SystemColumnNames.createdAt -> SoQLFixedTimestamp,
    SystemColumnNames.updatedAt -> SoQLFixedTimestamp
  )
  val typeContext = SoQLTypeContext

  def obfuscationContextFor(key: Array[Byte]) = new RowIdObfuscator(key)
  def generateObfuscationKey() = RowIdObfuscator.generateKey()
  val initialRowId = new RowId(0L)

  val sqlRepFor = SoQLRep.sqlRep _
  val jsonRepFor = SoQLRep.jsonRep { di => obfuscationContextFor(di.obfuscationKey) }

  def newRowLogCodec() = SoQLRowLogCodec

  def physicalColumnBaseBase(logicalColumnName: ColumnName, systemColumn: Boolean): String =
    AsciiIdentifierFilter(List(if(systemColumn) "s" else "u", logicalColumnName.name)).
      take(datasetMapLimits.maximumPhysicalColumnBaseLength).
      replaceAll("_+$", "").
      toLowerCase

  def isSystemColumnName(name: ColumnName) =
    name.caseFolded.startsWith(":")

  def universe: Managed[PostgresUniverse[CT, CV]] = new SimpleArm[PostgresUniverse[CT, CV]] {
    def flatMap[B](f: PostgresUniverse[CT, CV] => B): B = {
      val conn = dataSource.getConnection()
      try {
        conn.setAutoCommit(false)
        val u = new PostgresUniverse(conn, PostgresUniverseCommon)
        val result = f(u)
        conn.commit()
        result
      } finally {
        conn.close()
      }
    }
  }

  object PostgresUniverseCommon extends PostgresCommonSupport[SoQLType, SoQLValue] {
    val typeContext = common.typeContext

    val repFor = sqlRepFor

    val newRowCodec = common.newRowLogCodec _

    def isSystemColumn(ci: AbstractColumnInfoLike): Boolean =
      isSystemColumnName(ci.logicalName)

    def rowPreparer(transactionStart: DateTime, schema: ColumnIdMap[AbstractColumnInfoLike]): RowPreparer[SoQLValue] =
      new RowPreparer[SoQLValue] {
        def findCol(name: ColumnName) =
          schema.values.find(_.logicalName == name).getOrElse(sys.error(s"No $name column?")).systemId

        val idColumn = findCol(SystemColumnNames.id)
        val createdAtColumn = findCol(SystemColumnNames.createdAt)
        val updatedAtColumn = findCol(SystemColumnNames.updatedAt)

        def prepareForInsert(row: Row[SoQLValue], sid: RowId) = {
          val tmp = new MutableRow(row)
          tmp(idColumn) = SoQLID(sid.underlying)
          tmp(createdAtColumn) = SoQLFixedTimestamp(transactionStart)
          tmp(updatedAtColumn) = SoQLFixedTimestamp(transactionStart)
          tmp.freeze()
        }

        def prepareForUpdate(row: Row[SoQLValue]) = {
          val tmp = new MutableRow[SoQLValue](row)
          tmp -= idColumn
          tmp -= createdAtColumn
          tmp(updatedAtColumn) = SoQLFixedTimestamp(transactionStart)
          tmp.freeze()
        }
      }

    val executor: ExecutorService = common.executorService
    val obfuscationKeyGenerator: () => Array[Byte] = common.generateObfuscationKey _
    val initialRowId: RowId = common.initialRowId
    val tablespace: (String) => Option[String] = common.tableSpace
    val copyInProvider: (Connection, String, Reader) => Long = common.copyInProvider
    val timingReport = common.timingReport
  }

  object Mutator extends MutatorCommon[CT, CV] {
    def physicalColumnBaseBase(name: ColumnName, isSystemColumn: Boolean) =
      common.physicalColumnBaseBase(name, isSystemColumn)

    def isLegalLogicalName(identifier: ColumnName): Boolean =
      IdentifierFilter(identifier.name) == identifier.name &&
        identifier.name.length <= datasetMapLimits.maximumLogicalColumnNameLength &&
        !identifier.name.contains('$')

    def isSystemColumnName(identifier: ColumnName): Boolean =
      common.isSystemColumnName(identifier)

    val systemSchema: Map[ColumnName, CT] = common.systemSchema

    val systemIdColumnName: ColumnName =
      SystemColumnNames.id

    def typeNameFor(typ: CT): TypeName =
      typeContext.typeNamespace.userTypeForType(typ)

    def nameForTypeOpt(name: TypeName): Option[CT] =
      typeContext.typeNamespace.typeForUserType(name)

    def jsonRepFor(columnInfo: ColumnInfo[CT]): JsonColumnRep[CT, CV] =
      common.jsonRepFor(columnInfo)

    val schemaFinder = new SchemaFinder[CT, CV](universe, typeNameFor(_).name)
  }
}
