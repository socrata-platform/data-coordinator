package com.socrata.datacoordinator.truth.universe
package sql

import java.sql.Connection
import com.socrata.datacoordinator.truth.metadata.{GlobalLogPlayback, DatasetMapReader, PlaybackManifest, ColumnInfo}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.truth.RowLogCodec
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresGlobalLogPlayback, PostgresDatasetMapReader, PostgresSecondaryPlaybackManifest}
import com.socrata.datacoordinator.secondary.{SecondaryManifest, PlaybackToSecondary}
import com.socrata.datacoordinator.truth.loader.Delogger
import com.socrata.datacoordinator.truth.loader.sql.SqlDelogger
import com.socrata.datacoordinator.secondary.sql.{SqlSecondaryConfig, SqlSecondaryManifest}
import com.socrata.datacoordinator.util.TimingReport

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
    with SecondaryConfigProvider
{
  import typeInfo._

  def commit() { conn.commit() }

  def secondaryPlaybackManifest(storeId: String): PlaybackManifest =
    new PostgresSecondaryPlaybackManifest(conn, storeId)

  lazy val playbackToSecondary: PlaybackToSecondary[CT, CV] =
    new PlaybackToSecondary(conn, secondaryManifest, repFor, timingReport)

  def delogger(logTableName: String): Delogger[CV] =
    new SqlDelogger(conn, logTableName, newRowCodec _)

  lazy val secondaryManifest: SecondaryManifest =
    new SqlSecondaryManifest(conn)

  lazy val datasetMapReader: DatasetMapReader =
    new PostgresDatasetMapReader(conn)

  lazy val globalLogPlayback: GlobalLogPlayback =
    new PostgresGlobalLogPlayback(conn)

  lazy val secondaryConfig =
    new SqlSecondaryConfig(conn)
}
