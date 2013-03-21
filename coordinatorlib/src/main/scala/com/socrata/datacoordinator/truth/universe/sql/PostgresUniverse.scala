package com.socrata.datacoordinator.truth.universe
package sql

import java.sql.Connection

import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.truth.RowLogCodec
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresGlobalLogPlayback, PostgresDatasetMapReader, PostgresSecondaryPlaybackManifest}
import com.socrata.datacoordinator.secondary.{SecondaryManifest, PlaybackToSecondary}
import com.socrata.datacoordinator.truth.loader.{Logger, Delogger}
import com.socrata.datacoordinator.truth.loader.sql.{SqlLogger, SqlDelogger}
import com.socrata.datacoordinator.secondary.sql.{SqlSecondaryConfig, SqlSecondaryManifest}
import com.socrata.datacoordinator.util.TimingReport
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import org.joda.time.DateTime

trait TypeInfo[CT, CV] {
  def repFor(ci: ColumnInfo): SqlColumnRep[CT, CV]
  def newRowCodec(): RowLogCodec[CV]
}

final class PostgresUniverse[ColumnType, ColumnValue](conn: Connection, typeInfo: TypeInfo[ColumnType, ColumnValue], val timingReport: TimingReport)
  extends Universe[ColumnType, ColumnValue]
    with DatasetMapReaderProvider
    with GlobalLogPlaybackProvider
    with SecondaryManifestProvider
    with SecondaryPlaybackManifestProvider
    with PlaybackToSecondaryProvider
    with DeloggerProvider
    with LoggerProvider
    with SecondaryConfigProvider
{
  import typeInfo._

  private var loggerCache = Map.empty[String, Logger[CV]]
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
    new PlaybackToSecondary(conn, secondaryManifest, repFor, timingReport)

  def logger(datasetInfo: DatasetInfo): Logger[CV] = {
    val logName = datasetInfo.logTableName
    loggerCache.get(logName) match {
      case Some(logger) =>
        logger
      case None =>
        val logger = new SqlLogger(conn, logName, newRowCodec, timingReport)
        loggerCache += logName -> logger
        logger
    }
  }

  def delogger(datasetInfo: DatasetInfo): Delogger[CV] =
    new SqlDelogger(conn, datasetInfo.logTableName, newRowCodec _)

  lazy val secondaryManifest: SecondaryManifest =
    new SqlSecondaryManifest(conn)

  lazy val datasetMapReader: DatasetMapReader =
    new PostgresDatasetMapReader(conn, timingReport)

  lazy val globalLogPlayback: GlobalLogPlayback =
    new PostgresGlobalLogPlayback(conn)

  lazy val secondaryConfig =
    new SqlSecondaryConfig(conn, timingReport)
}
