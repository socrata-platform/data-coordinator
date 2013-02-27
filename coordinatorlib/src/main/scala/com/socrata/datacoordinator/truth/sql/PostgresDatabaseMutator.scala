package com.socrata.datacoordinator.truth
package sql

import java.sql.Connection
import javax.sql.DataSource

import org.joda.time.DateTime
import com.rojoma.simplearm.util._
import com.rojoma.simplearm.Managed

import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo, GlobalLog, DatasetMapWriter, DatasetInfo}
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresGlobalLog, PostgresDatasetMapWriter}
import com.socrata.datacoordinator.truth.loader.{DatasetContentsCopier, Logger, SchemaLoader, Loader, Report, RowPreparer}
import com.socrata.datacoordinator.truth.loader.sql.{RepBasedSqlSchemaLoader, RepBasedSqlDatasetContentsCopier, SqlLogger}
import com.socrata.datacoordinator.truth.{TypeContext, RowLogCodec}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.id.RowId
import com.socrata.id.numeric.IdProvider
import com.rojoma.simplearm.SimpleArm

// Does this need to be *Postgres*, or is all postgres-specific stuff encapsulated in its paramters?
class PostgresDatabaseMutator[CT, CV](dataSource: DataSource,
                                      repForColumn: ColumnInfo => SqlColumnRep[CT, CV],
                                      rowCodecFactory: () => RowLogCodec[CV],
                                      mapWriterFactory: Connection => DatasetMapWriter,
                                      globalLogFactory: Connection => GlobalLog,
                                      loaderFactory: (Connection, DateTime, CopyInfo, ColumnIdMap[ColumnInfo], IdProvider, Logger[CV]) => Loader[CV],
                                      tablespace: String => Option[String],
                                      rowFlushSize: Int = 128000,
                                      batchFlushSize: Int = 2000000)
  extends LowLevelDatabaseMutator[CV]
{
  type LoaderProvider = (CopyInfo, ColumnIdMap[ColumnInfo], RowPreparer[CV], IdProvider, Logger[CV], ColumnInfo => SqlColumnRep[CT, CV]) => Loader[CV]

  private class S(val conn: Connection, val datasetMap: DatasetMapWriter, val globalLog: GlobalLog) extends MutationContext {
    lazy val now = for {
      stmt <- managed(conn.createStatement())
      rs <- managed(stmt.executeQuery("SELECT current_timestamp"))
    } yield {
      rs.next()
      new DateTime(rs.getTimestamp(1).getTime)
    }

    def logger(datasetInfo: DatasetInfo): Logger[CV] =
      new SqlLogger(conn, datasetInfo.logTableName, rowCodecFactory, rowFlushSize, batchFlushSize)

    def schemaLoader(logger: Logger[CV]): SchemaLoader =
      new RepBasedSqlSchemaLoader(conn, logger, repForColumn, tablespace)

    def datasetContentsCopier(logger: Logger[CV]): DatasetContentsCopier =
      new RepBasedSqlDatasetContentsCopier(conn, logger, repForColumn)

    def withDataLoader[A](table: CopyInfo, schema: ColumnIdMap[ColumnInfo], logger: Logger[CV])(f: (Loader[CV]) => A): (Report[CV], RowId, A) = {
      val rowIdProvider = new com.socrata.datacoordinator.util.RowIdProvider(table.datasetInfo.nextRowId)
      using(loaderFactory(conn, now, table, schema, rowIdProvider, logger)) { loader =>
        val result = f(loader)
        val report = loader.report
        (report, rowIdProvider.finish(), result)
      }
    }
  }

  private def createInitialState(conn: Connection): S = {
    val datasetMap = mapWriterFactory(conn)
    val globalLog = globalLogFactory(conn)
    new S(conn, datasetMap, globalLog)
  }

  def openDatabase: Managed[MutationContext] = new SimpleArm[MutationContext] {
    def flatMap[A](f: MutationContext => A): A =
      using(dataSource.getConnection()) { conn =>
        conn.setAutoCommit(false)
        val result = f(createInitialState(conn))
        conn.commit()
        result
      }
  }
}
