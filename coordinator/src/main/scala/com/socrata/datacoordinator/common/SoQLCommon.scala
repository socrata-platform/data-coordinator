package com.socrata.datacoordinator.common

import com.socrata.datacoordinator.{Row, MutableRow}
import com.socrata.datacoordinator.service.{SchemaFinder, MutatorCommon}
import com.socrata.soql.types._
import com.socrata.soql.environment.{ColumnName, TypeName}
import com.socrata.soql.brita.{AsciiIdentifierFilter, IdentifierFilter}
import com.socrata.datacoordinator.common.soql.{SoQLRowLogCodec, SoQLRep, SoQLTypeContext}
import com.socrata.datacoordinator.truth.metadata.{DatasetCopyContext, DatasetInfo, AbstractColumnInfoLike, ColumnInfo}
import com.socrata.datacoordinator.truth.json.{JsonColumnWriteRep, JsonColumnRep, JsonColumnReadRep}
import java.util.concurrent.ExecutorService
import com.socrata.datacoordinator.truth.universe.sql.{PostgresUniverse, PostgresCommonSupport}
import org.joda.time.DateTime
import com.socrata.datacoordinator.util.collection.{MutableColumnIdSet, UserColumnIdMap, ColumnIdSet, ColumnIdMap}
import com.socrata.datacoordinator.truth.loader.RowPreparer
import com.socrata.datacoordinator.id.{UserColumnId, DatasetId, RowVersion, RowId}
import java.sql.Connection
import java.io.{File, OutputStream, Reader}
import com.socrata.datacoordinator.util.{NullCache, Cache, TransferrableContextTimingReport}
import javax.sql.DataSource
import com.rojoma.simplearm.{SimpleArm, Managed}
import com.socrata.soql.types.obfuscation.{Quadifier, CryptProvider}
import scala.concurrent.duration.Duration
import java.security.SecureRandom
import com.socrata.datacoordinator.truth.universe.CacheProvider

object SoQLSystemColumns { sc =>
  val id = new UserColumnId(":id")
  val createdAt = new UserColumnId(":created_at")
  val updatedAt = new UserColumnId(":updated_at")
  val version = new UserColumnId(":version")

  val schemaFragment = UserColumnIdMap(
    id -> SoQLID,
    version -> SoQLVersion,
    createdAt -> SoQLFixedTimestamp,
    updatedAt -> SoQLFixedTimestamp
  )

  val allSystemColumnIds = schemaFragment.keySet

  def isSystemColumnId(name: UserColumnId) =
    name.underlying.startsWith(":") && !name.underlying.startsWith(":@")
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
  def internalNameFromDatasetId(datasetId: DatasetId) =
    internalNamePrefix + datasetId.underlying

  val datasetMapLimits = StandardDatasetMapLimits

  val SystemColumns = SoQLSystemColumns

  val typeContext = SoQLTypeContext

  def idObfuscationContextFor(cryptProvider: CryptProvider) = new SoQLID.StringRep(cryptProvider)
  def versionObfuscationContextFor(cryptProvider: CryptProvider) = new SoQLVersion.StringRep(cryptProvider)
  def generateObfuscationKey() = CryptProvider.generateKey()
  val initialCounterValue = 0L

  val sqlRepFor = SoQLRep.sqlRep _
  def jsonReps(datasetInfo: DatasetInfo) = {
    val cp = new CryptProvider(datasetInfo.obfuscationKey)
    SoQLRep.jsonRep(idObfuscationContextFor(cp), versionObfuscationContextFor(cp))
  }

  def newRowLogCodec() = SoQLRowLogCodec

  def physicalColumnBaseBase(nameHint: String, systemColumn: Boolean): String =
    AsciiIdentifierFilter(List(if(systemColumn) "s" else "u", nameHint)).
      take(datasetMapLimits.maximumPhysicalColumnBaseLength).
      replaceAll("_+$", "").
      toLowerCase

  def isSystemColumnId(name: UserColumnId) =
    SoQLSystemColumns.isSystemColumnId(name)

  val universe: Managed[PostgresUniverse[CT, CV] with CacheProvider] = new SimpleArm[PostgresUniverse[CT, CV] with CacheProvider] {
    def flatMap[B](f: PostgresUniverse[CT, CV] with CacheProvider => B): B = {
      val conn = dataSource.getConnection()
      try {
        conn.setAutoCommit(false)
        val u = new PostgresUniverse(conn, PostgresUniverseCommon) with CacheProvider {
          val cache: Cache = common.cache
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

    val schemaFinder = new SchemaFinder[CT, CV](universe, typeContext.typeNamespace.userTypeForType)

    val allowDdlOnPublishedCopies = common.allowDdlOnPublishedCopies

    private val rng = new SecureRandom()
    def genUserColumnId() = {
      def quad() = Quadifier.quadify(rng.nextInt())
      new UserColumnId(quad() + "-" + quad())
    }
  }
}
