package com.socrata.datacoordinator.truth.universe
package sql

import java.util.concurrent.ExecutorService
import java.sql.Connection
import java.io.Reader

import org.joda.time.DateTime
import com.rojoma.simplearm.SimpleArm
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.sql.{PostgresDatabaseMutator, PostgresDatabaseReader, RepBasedSqlDatasetContext, SqlColumnRep}
import com.socrata.datacoordinator.truth._
import com.socrata.datacoordinator.truth.metadata.sql._
import com.socrata.datacoordinator.secondary.{SecondaryManifest, PlaybackToSecondary}
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.truth.loader.sql._
import com.socrata.datacoordinator.secondary.sql.{SqlSecondaryConfig, SqlSecondaryManifest}
import com.socrata.datacoordinator.util.{RowIdProvider, TransferrableContextTimingReport}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import scala.concurrent.duration.Duration
import com.socrata.datacoordinator.truth.metadata.DatasetInfo
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.id.RowId

trait CommonSupport[CT, CV] {
  val executor: ExecutorService
  val typeContext: TypeContext[CT, CV]
  def repFor(ci: ColumnInfo[CT]): SqlColumnRep[CT, CV]
  def newRowCodec(): RowLogCodec[CV]
  def isSystemColumn(ci: AbstractColumnInfoLike): Boolean

  val obfuscationKeyGenerator: () => Array[Byte]
  val initialRowId: RowId
  val tablespace: String => Option[String]
  val copyInProvider: (Connection, String, Reader) => Long

  def rowPreparer(transactionStart: DateTime, schema: ColumnIdMap[AbstractColumnInfoLike]): RowPreparer[CV]

  lazy val loaderProvider = new AbstractSqlLoaderProvider(executor, typeContext, repFor, isSystemColumn) with PostgresSqlLoaderProvider[CT, CV] {
    def copyIn(conn: Connection, sql: String, reader: Reader): Long =
      copyInProvider(conn, sql, reader)
  }
}

object PostgresCopyIn extends ((Connection, String, Reader) => Long) {
  def apply(conn: Connection, sql: String, csv: Reader): Long =
    conn.asInstanceOf[org.postgresql.PGConnection].getCopyAPI.copyIn(sql, csv)
}

object C3P0WrappedPostgresCopyIn extends ((Connection, String, Reader) => Long) {
  import com.mchange.v2.c3p0.C3P0ProxyConnection

  private val method = PostgresCopyIn.getClass.getMethod("apply", classOf[Connection], classOf[String], classOf[Reader])

  def apply(conn: Connection, sql: String, csv: Reader): Long =
    conn.asInstanceOf[C3P0ProxyConnection].rawConnectionOperation(method, PostgresCopyIn, Array(C3P0ProxyConnection.RAW_CONNECTION, sql, csv)).asInstanceOf[java.lang.Long].longValue
}

class PostgresUniverse[ColumnType, ColumnValue](conn: Connection,
                                                commonSupport: CommonSupport[ColumnType, ColumnValue],
                                                val timingReport: TransferrableContextTimingReport,
                                                user: String)
  extends Universe[ColumnType, ColumnValue]
    with DatasetMapReaderProvider
    with DatasetMapWriterProvider
    with GlobalLogPlaybackProvider
    with SecondaryManifestProvider
    with SecondaryPlaybackManifestProvider
    with PlaybackToSecondaryProvider
    with DeloggerProvider
    with LoggerProvider
    with SecondaryConfigProvider
    with PrevettedLoaderProvider
    with LoaderProvider
    with TruncatorProvider
    with DatasetContentsCopierProvider
    with SchemaLoaderProvider
    with GlobalLogProvider
    with DatasetReaderProvider
    with DatasetMutatorProvider
{
  import commonSupport._

  private var loggerCache = Map.empty[String, Logger[CT, CV]]
  private var txnStart = DateTime.now()

  def commit() {
    loggerCache.values.foreach(_.close())
    loggerCache = Map.empty
    conn.commit()
    txnStart = DateTime.now()
  }

  def transactionStart = txnStart

  def secondaryPlaybackManifest(storeId: String): PlaybackManifest =
    new PostgresSecondaryPlaybackManifest(conn, storeId)

  lazy val playbackToSecondary: PlaybackToSecondary[CT, CV] =
    new PlaybackToSecondary(conn, secondaryManifest, typeContext.typeNamespace, repFor, timingReport)

  def logger(datasetInfo: DatasetInfo): Logger[CT, CV] = {
    val logName = datasetInfo.logTableName
    loggerCache.get(logName) match {
      case Some(logger) =>
        logger
      case None =>
        val logger = new SqlLogger[CT, CV](conn, logName, newRowCodec, timingReport)
        loggerCache += logName -> logger
        logger
    }
  }

  def delogger(datasetInfo: DatasetInfo): Delogger[CV] =
    new SqlDelogger(conn, datasetInfo.logTableName, newRowCodec _)

  lazy val secondaryManifest: SecondaryManifest =
    new SqlSecondaryManifest(conn)

  lazy val truncator =
    new SqlTruncator(conn)

  lazy val datasetMapReader: DatasetMapReader[CT] =
    new PostgresDatasetMapReader(conn, typeContext.typeNamespace, timingReport)

  lazy val datasetMapWriter: DatasetMapWriter[CT] =
    new PostgresDatasetMapWriter(conn, typeContext.typeNamespace, timingReport, obfuscationKeyGenerator, initialRowId)

  lazy val globalLogPlayback: GlobalLogPlayback =
    new PostgresGlobalLogPlayback(conn)

  lazy val secondaryConfig =
    new SqlSecondaryConfig(conn, timingReport)

  lazy val globalLog =
    new PostgresGlobalLog(conn)

  def datasetContextFactory(schema: ColumnIdMap[ColumnInfo[CT]]): RepBasedSqlDatasetContext[CT, CV] = {
    RepBasedSqlDatasetContext(
      typeContext,
      schema.mapValuesStrict(repFor),
      schema.values.find(_.isUserPrimaryKey).map(_.systemId),
      schema.values.find(_.isSystemPrimaryKey).getOrElse(sys.error("No system primary key?")).systemId,
      schema.keySet.filter { cid => isSystemColumn(schema(cid)) }
    )
  }

  def sqlizerFactory(copyInfo: CopyInfo, datasetContext: RepBasedSqlDatasetContext[CT, CV]) =
    new PostgresRepBasedDataSqlizer(copyInfo.dataTableName, datasetContext, executor, copyInProvider)

  def prevettedLoader(copyCtx: DatasetCopyContext[CT], logger: Logger[CT, CV]) =
    new SqlPrevettedLoader(conn, sqlizerFactory(copyCtx.copyInfo, datasetContextFactory(copyCtx.schema)), logger)

  def loader(copyCtx: DatasetCopyContext[CT], rowIdProvider: RowIdProvider, logger: Logger[CT, CV]) =
    managed(loaderProvider(conn, copyCtx, rowPreparer(transactionStart, copyCtx.schema), rowIdProvider, logger, timingReport))

  lazy val lowLevelDatabaseReader = new PostgresDatabaseReader(conn, datasetMapReader, repFor)

  lazy val datasetReader = DatasetReader(lowLevelDatabaseReader)

  lazy val lowLevelDatabaseMutator = new PostgresDatabaseMutator[ColumnType, ColumnValue](
    new SimpleArm[PostgresUniverse.this.type] {
      def flatMap[B](f: PostgresUniverse.this.type => B): B = f(PostgresUniverse.this)
    }
  )

  lazy val datasetMutator: DatasetMutator[CT, CV] =
    DatasetMutator(lowLevelDatabaseMutator, Duration(10, "s"))

  def schemaLoader(datasetInfo: DatasetInfo) =
    new RepBasedPostgresSchemaLoader(conn, logger(datasetInfo), repFor, tablespace)

  def datasetContentsCopier(datasetInfo: DatasetInfo): DatasetContentsCopier[CT] =
    new RepBasedSqlDatasetContentsCopier(conn, logger(datasetInfo), repFor, timingReport)
}
