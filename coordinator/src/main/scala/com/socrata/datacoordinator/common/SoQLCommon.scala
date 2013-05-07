package com.socrata.datacoordinator.common

import com.socrata.datacoordinator.{Row, MutableRow}
import com.socrata.datacoordinator.service.{SchemaFinder, MutatorCommon}
import com.socrata.soql.types._
import com.socrata.soql.environment.{ColumnName, TypeName}
import com.socrata.soql.brita.{AsciiIdentifierFilter, IdentifierFilter}
import com.socrata.datacoordinator.common.soql.{SoQLRowLogCodec, SoQLRep, SoQLTypeContext}
import com.socrata.datacoordinator.common.util.{RowVersionObfuscator, CryptProvider, RowIdObfuscator}
import com.socrata.datacoordinator.truth.metadata.{DatasetCopyContext, DatasetInfo, AbstractColumnInfoLike, ColumnInfo}
import com.socrata.datacoordinator.truth.json.{JsonColumnWriteRep, JsonColumnRep, JsonColumnReadRep}
import java.util.concurrent.ExecutorService
import com.socrata.datacoordinator.truth.universe.sql.{PostgresUniverse, PostgresCommonSupport}
import org.joda.time.DateTime
import com.socrata.datacoordinator.util.collection.{ColumnIdSet, ColumnIdMap}
import com.socrata.datacoordinator.truth.loader.{RowPreparerDeclinedUpsert, VersionMismatch, RowPreparer}
import com.socrata.datacoordinator.id.{RowVersion, RowId}
import java.sql.Connection
import java.io.Reader
import com.socrata.datacoordinator.util.{RowDataProvider, TransferrableContextTimingReport}
import javax.sql.DataSource
import com.rojoma.simplearm.{SimpleArm, Managed}
import com.socrata.datacoordinator.truth.DatasetContext

object SoQLSystemColumns { sc =>
  val id = ColumnName(":id")
  val createdAt = ColumnName(":created_at")
  val updatedAt = ColumnName(":updated_at")
  val version = ColumnName(":version")

  val schemaFragment = Map(
    id -> SoQLID,
    version -> SoQLVersion,
    createdAt -> SoQLFixedTimestamp,
    updatedAt -> SoQLFixedTimestamp
  )

  val allSystemColumnNames = schemaFragment.keySet
}

class SoQLCommon(dataSource: DataSource,
                 copyInProvider: (Connection, String, Reader) => Long,
                 executorService: ExecutorService,
                 tableSpace: String => Option[String],
                 val timingReport: TransferrableContextTimingReport,
                 allowDdlOnPublishedCopies: Boolean)
{ common =>
  type CT = SoQLType
  type CV = SoQLValue

  val datasetMapLimits = StandardDatasetMapLimits

  val SystemColumns = SoQLSystemColumns

  val typeContext = SoQLTypeContext

  def idObfuscationContextFor(cryptProvider: CryptProvider) = new RowIdObfuscator(cryptProvider)
  def versionObfuscationContextFor(cryptProvider: CryptProvider) = new RowVersionObfuscator(cryptProvider)
  def generateObfuscationKey() = CryptProvider.generateKey()
  val initialCounterValue = 0L

  val sqlRepFor = SoQLRep.sqlRep _
  def jsonReps(datasetInfo: DatasetInfo) = {
    val cp = new CryptProvider(datasetInfo.obfuscationKey)
    SoQLRep.jsonRep(idObfuscationContextFor(cp), versionObfuscationContextFor(cp))
  }

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

    def rowPreparer(transactionStart: DateTime, ctx: DatasetCopyContext[CT], idProvider: RowDataProvider, replaceUpdatedRows: Boolean): RowPreparer[SoQLValue] =
      new RowPreparer[SoQLValue] {
        val schema = ctx.schema
        lazy val jsonRepFor = jsonReps(ctx.datasetInfo)

        def findCol(name: ColumnName) =
          schema.values.find(_.logicalName == name).getOrElse(sys.error(s"No $name column?")).systemId

        val idColumn = findCol(SystemColumns.id)
        val createdAtColumn = findCol(SystemColumns.createdAt)
        val updatedAtColumn = findCol(SystemColumns.updatedAt)
        val versionColumn = findCol(SystemColumns.version)

        val columnsRequiredForDelete = ColumnIdSet(versionColumn)

        val primaryKeyColumn = ctx.pkCol_!

        assert(schema(versionColumn).typeName == typeContext.typeNamespace.nameForType(SoQLVersion))

        val allSystemColumns = ColumnIdSet(SystemColumns.allSystemColumnNames.toSeq.map(findCol) : _*)

        def prepareForInsert(row: Row[SoQLValue], sid: RowId) = {
          val tmp = new MutableRow(row)
          tmp(idColumn) = SoQLID(sid.underlying)
          tmp(createdAtColumn) = SoQLFixedTimestamp(transactionStart)
          tmp(updatedAtColumn) = SoQLFixedTimestamp(transactionStart)
          tmp(versionColumn) = SoQLVersion(idProvider.allocateVersion().underlying)
          Right(tmp.freeze())
        }

        def baseRow(oldRow: Row[SoQLValue]): MutableRow[SoQLValue] =
          if(replaceUpdatedRows) {
            val blank = new MutableRow[SoQLValue]
            for(cid <- allSystemColumns.iterator) {
              if(oldRow.contains(cid)) blank(cid) = oldRow(cid)
            }
            blank
          } else {
            new MutableRow[SoQLValue](oldRow)
          }

        def versionOf(row: Row[SoQLValue]): Option[RowVersion] =
          row.get(versionColumn) match {
            case Some(SoQLVersion(v)) => Some(new RowVersion(v))
            case Some(SoQLNull) => None
            case Some(other) => sys.error("Bad type in version column: " + other.typ)
            case None => None
          }

        def prepareForUpdate(row: Row[SoQLValue], oldRow: Row[SoQLValue]): Either[RowPreparerDeclinedUpsert[CV], Row[CV]] = {
          for {
            oldVer <- versionOf(oldRow)
            newVer <- versionOf(row)
          } if(oldVer != newVer) return Left(VersionMismatch(row(primaryKeyColumn.systemId), oldVer, newVer))
          val tmp = baseRow(oldRow)
          val rowIt = row.iterator
          while(rowIt.hasNext) {
            rowIt.advance()
            if(!allSystemColumns(rowIt.key)) tmp(rowIt.key) = rowIt.value
          }
          tmp(updatedAtColumn) = SoQLFixedTimestamp(transactionStart)
          tmp(versionColumn) = SoQLVersion(idProvider.allocateVersion().underlying)
          Right(tmp.freeze())
        }

        def prepareForDelete(id: CV, requestedVersion: Option[RowVersion], existingVersion: RowVersion): Option[RowPreparerDeclinedUpsert[CV]] =
          for {
            newVer <- requestedVersion
            if existingVersion != newVer
          } yield VersionMismatch(id, existingVersion, newVer)
      }

    val executor: ExecutorService = common.executorService
    val obfuscationKeyGenerator: () => Array[Byte] = common.generateObfuscationKey _
    val initialCounterValue: Long = common.initialCounterValue
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

    val systemSchema: Map[ColumnName, CT] = common.SystemColumns.schemaFragment

    val systemIdColumnName: ColumnName =
      SystemColumns.id

    val versionColumnName: ColumnName =
      SystemColumns.version

    def typeNameFor(typ: CT): TypeName =
      typeContext.typeNamespace.userTypeForType(typ)

    def nameForTypeOpt(name: TypeName): Option[CT] =
      typeContext.typeNamespace.typeForUserType(name)

    def jsonReps(di: DatasetInfo): CT => JsonColumnRep[CT, CV] = common.jsonReps(di)

    val schemaFinder = new SchemaFinder[CT, CV](universe, typeNameFor)

    def makeValueFromRowVersion(rv: RowVersion) =
      typeContext.makeValueFromRowVersion(rv)

    val allowDdlOnPublishedCopies = common.allowDdlOnPublishedCopies
  }
}
