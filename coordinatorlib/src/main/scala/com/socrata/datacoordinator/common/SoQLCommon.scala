package com.socrata.datacoordinator.common

import com.rojoma.simplearm.{Managed, SimpleArm}
import com.socrata.datacoordinator.common.soql.{SoQLRep, SoQLRowLogCodec, SoQLTypeContext}
import com.socrata.datacoordinator.id._
import com.socrata.datacoordinator.truth.json.{JsonColumnReadRep, JsonColumnRep, JsonColumnWriteRep}
import com.socrata.datacoordinator.truth.loader.RowPreparer
import com.socrata.datacoordinator.truth.metadata.{AbstractColumnInfoLike, ColumnInfo, DatasetCopyContext, DatasetInfo}
import com.socrata.datacoordinator.truth.universe.sql.{PostgresCommonSupport, PostgresUniverse}
import com.socrata.datacoordinator.truth.universe.{CacheProvider, SchemaFinderProvider}
import com.socrata.datacoordinator.util.collection.{ColumnIdMap, ColumnIdSet, MutableColumnIdSet, UserColumnIdMap}
import com.socrata.datacoordinator.util.{Cache, NullCache, TransferrableContextTimingReport}
import com.socrata.datacoordinator.{MutableRow, Row}
import com.socrata.soql.brita.{AsciiIdentifierFilter, IdentifierFilter}
import com.socrata.soql.environment.{ColumnName, TypeName}
import com.socrata.soql.types._
import com.socrata.soql.types.obfuscation.{CryptProvider, Quadifier}
import java.io.{File, OutputStream, Reader}
import java.security.SecureRandom
import java.sql.Connection
import java.util.concurrent.ExecutorService
import javax.sql.DataSource

import com.socrata.soql.SoQLAnalyzer
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import org.joda.time.DateTime

import scala.concurrent.duration.{Duration, FiniteDuration}

object SoQLSystemColumns { sc =>
  val id = new UserColumnId(":id")
  val createdAt = new UserColumnId(":created_at")
  val updatedAt = new UserColumnId(":updated_at")
  val version = new UserColumnId(":version")

  val schemaFragment = UserColumnIdMap[MutatorColumnInfo[SoQLType]](
    id -> MutatorColInfo(SoQLID, id),
    version -> MutatorColInfo(SoQLVersion, version),
    createdAt -> MutatorColInfo(SoQLFixedTimestamp, createdAt),
    updatedAt -> MutatorColInfo(SoQLFixedTimestamp, updatedAt)
  )

  val allSystemColumnIds = schemaFragment.keySet

  def isSystemColumnId(name: UserColumnId): Boolean =
    name.underlying.startsWith(":") && !name.underlying.startsWith(":@")

  case class MutatorColInfo(typ: SoQLType, id: UserColumnId) extends MutatorColumnInfo[SoQLType] {
    def fieldName = Some(ColumnName(id.underlying))
    def computationStrategy = None
  }
}

class SoQLCommon(dataSource: DataSource,
                 copyInProvider: (Connection, String, OutputStream => Unit) => Long,
                 executorService: ExecutorService,
                 tableSpace: String => Option[String],
                 val timingReport: TransferrableContextTimingReport,
                 allowDdlOnPublishedCopies: Boolean,
                 writeLockTimeout: Duration,
                 instance: String,
                 tmpDir: File,
                 logTableCleanupDeleteOlderThan: FiniteDuration,
                 logTableCleanupDeleteEvery: FiniteDuration,
                 //tableCleanupDelay: FiniteDuration,
                 cache: Cache)
{ common =>
  type CT = SoQLType
  type CV = SoQLValue

  private val internalNamePrefix = instance + "."
  def datasetIdFromInternalName(internalName: String): Option[DatasetId] = {
    if(internalName.startsWith(instance)) {
      try {
        Some(new DatasetId(internalName.substring(internalNamePrefix.length).toLong))
      } catch {
        case _: NumberFormatException => None
      }
    } else {
      None
    }
  }
  def internalNameFromDatasetId(datasetId: DatasetId): String =
    internalNamePrefix + datasetId.underlying

  val datasetMapLimits = StandardDatasetMapLimits

  val SystemColumns = SoQLSystemColumns

  val typeContext = SoQLTypeContext

  def idObfuscationContextFor(cryptProvider: CryptProvider) = new SoQLID.StringRep(cryptProvider)
  def versionObfuscationContextFor(cryptProvider: CryptProvider) = new SoQLVersion.StringRep(cryptProvider)
  def generateObfuscationKey(): Array[Byte] = CryptProvider.generateKey()
  val initialCounterValue = 0L

  val sqlRepFor = SoQLRep.sqlRep _
  def jsonReps(datasetInfo: DatasetInfo): (SoQLType => JsonColumnRep[SoQLType, SoQLValue]) = {
    val cp = new CryptProvider(datasetInfo.obfuscationKey)
    SoQLRep.jsonRep(idObfuscationContextFor(cp), versionObfuscationContextFor(cp))
  }

  def newRowLogCodec() = SoQLRowLogCodec

  def physicalColumnBaseBase(nameHint: String, systemColumn: Boolean): String =
    AsciiIdentifierFilter(List(if(systemColumn) "s" else "u", nameHint)).
      take(datasetMapLimits.maximumPhysicalColumnBaseLength).
      replaceAll("_+$", "").
      toLowerCase

  def isSystemColumnId(name: UserColumnId): Boolean =
    SoQLSystemColumns.isSystemColumnId(name)

  val universe: Managed[PostgresUniverse[CT, CV] with SchemaFinderProvider] = new SimpleArm[PostgresUniverse[CT, CV] with SchemaFinderProvider] {
    def flatMap[B](f: PostgresUniverse[CT, CV] with SchemaFinderProvider => B): B = {
      val conn = dataSource.getConnection()
      try {
        conn.setAutoCommit(false)
        val u = new PostgresUniverse(conn, PostgresUniverseCommon) with SchemaFinderProvider {
          lazy val cache: Cache = common.cache
          lazy val schemaFinder = new SchemaFinder(common.typeContext.typeNamespace.userTypeForType, cache)
        }
        try {
          val result = f(u)
          u.commit()
          result
        } finally {
          u.rollback()
        }
      } finally {
        conn.close()
      }
    }
  }

  object PostgresUniverseCommon extends PostgresCommonSupport[SoQLType, SoQLValue] {
    val typeContext = common.typeContext

    val repFor = sqlRepFor

    val tmpDir = common.tmpDir

    //val tableCleanupDelay = common.tableCleanupDelay
    val logTableCleanupDeleteOlderThan = common.logTableCleanupDeleteOlderThan
    val logTableCleanupDeleteEvery = common.logTableCleanupDeleteEvery
    val newRowCodec = common.newRowLogCodec _

    def isSystemColumn(ci: AbstractColumnInfoLike): Boolean =
      isSystemColumnId(ci.userColumnId)

    val datasetIdFormatter = internalNameFromDatasetId _

    val writeLockTimeout = common.writeLockTimeout

    def rowPreparer(transactionStart: DateTime, ctx: DatasetCopyContext[CT], replaceUpdatedRows: Boolean): RowPreparer[SoQLValue] =
      new RowPreparer[SoQLValue] {
        val schema = ctx.schemaByUserColumnId
        lazy val jsonRepFor = jsonReps(ctx.datasetInfo)

        def findCol(name: UserColumnId) =
          schema.getOrElse(name, sys.error(s"No $name column?")).systemId

        val idColumn = findCol(SystemColumns.id)
        val createdAtColumn = findCol(SystemColumns.createdAt)
        val updatedAtColumn = findCol(SystemColumns.updatedAt)
        val versionColumn = findCol(SystemColumns.version)

        val columnsRequiredForDelete = ColumnIdSet(versionColumn)

        val primaryKeyColumn = ctx.pkCol_!

        assert(ctx.schema(versionColumn).typeName == typeContext.typeNamespace.nameForType(SoQLVersion))

        val allSystemColumns = locally {
          val result = MutableColumnIdSet()
          for(c <- SystemColumns.allSystemColumnIds) {
            result += findCol(c)
          }
          result.freeze()
        }

        def prepareForInsert(row: Row[SoQLValue], sid: RowId, version: RowVersion) = {
          val tmp = new MutableRow(row)
          tmp(idColumn) = SoQLID(sid.underlying)
          tmp(createdAtColumn) = SoQLFixedTimestamp(transactionStart)
          tmp(updatedAtColumn) = SoQLFixedTimestamp(transactionStart)
          tmp(versionColumn) = SoQLVersion(version.underlying)
          tmp.freeze()
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

        def prepareForUpdate(row: Row[SoQLValue], oldRow: Row[SoQLValue], newVersion: RowVersion): Row[CV] = {
          val tmp = baseRow(oldRow)
          val rowIt = row.iterator
          while(rowIt.hasNext) {
            rowIt.advance()
            if(!allSystemColumns(rowIt.key)) tmp(rowIt.key) = rowIt.value
          }
          tmp(updatedAtColumn) = SoQLFixedTimestamp(transactionStart)
          tmp(versionColumn) = SoQLVersion(newVersion.underlying)
          tmp.freeze()
        }
      }

    val executor: ExecutorService = common.executorService
    val obfuscationKeyGenerator: () => Array[Byte] = common.generateObfuscationKey _
    val initialCounterValue: Long = common.initialCounterValue
    val tablespace: (String) => Option[String] = common.tableSpace
    val copyInProvider: (Connection, String, OutputStream => Unit) => Long = common.copyInProvider
    val timingReport = common.timingReport

    def soqlAnalyzer: SoQLAnalyzer[SoQLType] = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)
  }

  object Mutator extends MutatorCommon[CT, CV] {
    def physicalColumnBaseBase(nameHint: String, isSystemColumn: Boolean) =
      common.physicalColumnBaseBase(nameHint, isSystemColumn)

    def isSystemColumnId(identifier: UserColumnId): Boolean =
      common.isSystemColumnId(identifier)

    val systemSchema = common.SystemColumns.schemaFragment

    val systemIdColumnId = SystemColumns.id

    val versionColumnId = SystemColumns.version

    def jsonReps(di: DatasetInfo): CT => JsonColumnRep[CT, CV] = common.jsonReps(di)

    val typeContext = common.typeContext

    val allowDdlOnPublishedCopies = common.allowDdlOnPublishedCopies

    private val rng = new SecureRandom()
    def genUserColumnId() = {
      def quad() = Quadifier.quadify(rng.nextInt())
      new UserColumnId(quad() + "-" + quad())
    }
  }
}
