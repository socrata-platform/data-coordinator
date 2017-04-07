package com.socrata.datacoordinator.truth.universe
package sql

import java.util.concurrent.ExecutorService
import java.sql.Connection
import java.io.{File, OutputStream, Reader}

import com.socrata.datacoordinator.secondary.messaging.MessageProducer
import org.joda.time.DateTime
import com.rojoma.simplearm.SimpleArm
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.sql.{PostgresDatabaseMutator, PostgresDatabaseReader, RepBasedSqlDatasetContext, SqlColumnRep}
import com.socrata.datacoordinator.truth._
import com.socrata.datacoordinator.truth.metadata.sql._
import com.socrata.datacoordinator.secondary.{SecondaryReplicationMessages, PlaybackToSecondary, SecondaryManifest}
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.truth.loader.sql._
import com.socrata.datacoordinator.secondary.sql.{SqlSecondaryManifest, SqlSecondaryStoresConfig}
import com.socrata.datacoordinator.util._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{Duration, FiniteDuration}
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.metadata.DatasetInfo
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.soql.SoQLAnalyzer
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.typechecker.FunctionInfo

trait PostgresCommonSupport[CT, CV] {

  def soqlAnalyzer: SoQLAnalyzer[CT]
  val executor: ExecutorService
  val typeContext: TypeContext[CT, CV]
  def repFor: ColumnInfo[CT] => SqlColumnRep[CT, CV]
  def newRowCodec: () => RowLogCodec[CV]
  def isSystemColumn(ci: AbstractColumnInfoLike): Boolean

  val obfuscationKeyGenerator: () => Array[Byte]
  val initialCounterValue: Long
  val tablespace: String => Option[String]
  val copyInProvider: (Connection, String, OutputStream => Unit) => Long
  val timingReport: TransferrableContextTimingReport

  val datasetIdFormatter: DatasetId => String

  def rowPreparer(transactionStart: DateTime, ctx: DatasetCopyContext[CT], replaceUpdatedRows: Boolean): RowPreparer[CV]

  def writeLockTimeout: Duration
   
  def tmpDir: File
  
  def logTableCleanupDeleteOlderThan: FiniteDuration
  def logTableCleanupDeleteEvery: FiniteDuration
  //def tableCleanupDelay: FiniteDuration

  lazy val loaderProvider = new AbstractSqlLoaderProvider(executor, typeContext, repFor, isSystemColumn) with PostgresSqlLoaderProvider[CT, CV] {
    def copyIn(conn: Connection, sql: String, output: OutputStream => Unit): Long =
      copyInProvider(conn, sql, output)
  }
}

object PostgresCopyIn extends ((Connection, String, OutputStream => Unit) => Long) {
  def apply(conn: Connection, sql: String, output: OutputStream => Unit): Long =
    PostgresRepBasedDataSqlizer.pgCopyManager(conn, sql, output)
}

object C3P0WrappedPostgresCopyIn extends ((Connection, String, OutputStream => Unit) => Long) {
  import com.mchange.v2.c3p0.C3P0ProxyConnection

  private val method = PostgresCopyIn.getClass.getMethod("apply", classOf[Connection], classOf[String], classOf[OutputStream => Unit])

  def apply(conn: Connection, sql: String, output: OutputStream => Unit): Long =
    conn.asInstanceOf[C3P0ProxyConnection].rawConnectionOperation(method, PostgresCopyIn, Array(C3P0ProxyConnection.RAW_CONNECTION, sql, output)).asInstanceOf[java.lang.Long].longValue
}

class PostgresUniverse[ColumnType, ColumnValue](conn: Connection,
                                                commonSupport: PostgresCommonSupport[ColumnType, ColumnValue])
  extends Universe[ColumnType, ColumnValue]
    with Commitable
    with DatasetMapReaderProvider
    with DatasetMapWriterProvider
    with SecondaryManifestProvider
    with PlaybackToSecondaryProvider
    with SecondaryReplicationMessagesProvider
    with DeloggerProvider
    with LoggerProvider
    with SecondaryStoresConfigProvider
    with PrevettedLoaderProvider
    with LoaderProvider
    with TruncatorProvider
    with DatasetContentsCopierProvider
    with SchemaLoaderProvider
    with DatasetReaderProvider
    with DatasetMutatorProvider
    with DatasetDropperProvider
    with TableCleanupProvider
    with LogTableCleanupProvider
    with SoQLAnalyzerProvider
{
  import commonSupport._

  private val log = LoggerFactory.getLogger(classOf[PostgresUniverse[ColumnType, ColumnValue]])

  private var loggerCache = Map.empty[String, Logger[CT, CV]]
  private var txnStart = DateTime.now()

  private def finish(op: Connection => Unit) {
    loggerCache.values.foreach(_.close())
    loggerCache = Map.empty
    op(conn)
    // set transaction back to default isolation level
    if (conn.getTransactionIsolation != Connection.TRANSACTION_READ_COMMITTED) {
      log.info("Changing transaction isolation level back to READ COMMITTED")
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
    }
    txnStart = DateTime.now()
  }

  def commit() {
    finish(_.commit())
  }

  def rollback() {
    finish(_.rollback())
  }

  def transactionStart = txnStart

  lazy val playbackToSecondary: PlaybackToSecondary[CT, CV] =
    new PlaybackToSecondary(this, repFor, typeContext.typeNamespace.typeForUserType, datasetIdFormatter, timingReport)

  def secondaryReplicationMessages(producer: MessageProducer): SecondaryReplicationMessages[CT, CV] =
    new SecondaryReplicationMessages(this, producer)

  def logger(datasetInfo: DatasetInfo, user: String): Logger[CT, CV] = {
    val logName = datasetInfo.logTableName
    loggerCache.get(logName) match {
      case Some(logger) =>
        logger
      case None =>
        val logger = new PostgresLogger[CT, CV](conn, datasetInfo.auditTableName, user, logName, newRowCodec, timingReport, copyInProvider, tmpDir) with LeakDetect
        loggerCache += logName -> logger
        logger
    }
  }

  def delogger(datasetInfo: DatasetInfo): Delogger[CV] =
    new SqlDelogger(conn, datasetInfo.logTableName, newRowCodec)

  lazy val secondaryManifest: SecondaryManifest =
    new SqlSecondaryManifest(conn)

  lazy val truncator =
    new SqlTruncator(conn)

  lazy val datasetMapReader: DatasetMapReader[CT] =
    new PostgresDatasetMapReader(conn, typeContext.typeNamespace, timingReport)

  lazy val datasetMapWriter: DatasetMapWriter[CT] =
    new PostgresDatasetMapWriter(conn, typeContext.typeNamespace, timingReport, obfuscationKeyGenerator, initialCounterValue)

  lazy val secondaryStoresConfig =
    new SqlSecondaryStoresConfig(conn, timingReport)

  def datasetContextFactory(schema: ColumnIdMap[ColumnInfo[CT]]): RepBasedSqlDatasetContext[CT, CV] = {
    RepBasedSqlDatasetContext(
      typeContext,
      schema.mapValuesStrict(repFor),
      schema.values.find(_.isUserPrimaryKey).map(_.systemId),
      schema.values.find(_.isSystemPrimaryKey).getOrElse(sys.error("No system primary key?")).systemId,
      schema.values.find(_.isVersion).getOrElse(sys.error("No version column?")).systemId,
      schema.keySet.filter { cid => isSystemColumn(schema(cid)) }
    )
  }

  def sqlizerFactory(copyInfo: CopyInfo, datasetContext: RepBasedSqlDatasetContext[CT, CV]) =
    new PostgresRepBasedDataSqlizer(copyInfo.dataTableName, datasetContext, copyInProvider)

  def prevettedLoader(copyCtx: DatasetCopyContext[CT], logger: Logger[CT, CV]) =
    new SqlPrevettedLoader(conn, sqlizerFactory(copyCtx.copyInfo, datasetContextFactory(copyCtx.schema)), logger)

  def loader(copyCtx: DatasetCopyContext[CT], rowIdProvider: RowIdProvider, rowVersionProvider: RowVersionProvider, logger: Logger[CT, CV], reportWriter: ReportWriter[CV], replaceUpdatedRows: Boolean, updateOnly: Boolean) =
    managed(loaderProvider(conn, copyCtx, rowPreparer(transactionStart, copyCtx, replaceUpdatedRows), updateOnly, rowIdProvider, rowVersionProvider, logger, reportWriter, timingReport))

  lazy val lowLevelDatabaseReader = new PostgresDatabaseReader(conn, datasetMapReader, repFor)

  lazy val datasetReader = DatasetReader(lowLevelDatabaseReader)

  lazy val lowLevelDatabaseMutator = new PostgresDatabaseMutator[ColumnType, ColumnValue](
    new SimpleArm[PostgresUniverse.this.type] {
      def flatMap[B](f: PostgresUniverse.this.type => B): B = f(PostgresUniverse.this)
    }
  )

  lazy val datasetMutator: DatasetMutator[CT, CV] =
    DatasetMutator(lowLevelDatabaseMutator, writeLockTimeout, soqlAnalyzer)

  def schemaLoader(logger: Logger[CT, CV]) =
    new RepBasedPostgresSchemaLoader(conn, logger, repFor, tablespace)

  def datasetContentsCopier(logger: Logger[CT, CV]): DatasetContentsCopier[CT] =
    new RepBasedSqlDatasetContentsCopier(conn, logger, repFor, timingReport)

  lazy val datasetDropper =
    new SqlDatasetDropper(conn, writeLockTimeout, datasetMapWriter)

  lazy val tableCleanup: TableCleanup =
    new SqlTableCleanup(conn) //, tableCleanupDelay)

  lazy val logTableCleanup: LogTableCleanup =
    new SqlLogTableCleanup(conn, logTableCleanupDeleteOlderThan, logTableCleanupDeleteEvery)

  def soqlAnalyzer: SoQLAnalyzer[CT] = commonSupport.soqlAnalyzer
}
