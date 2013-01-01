package com.socrata.datacoordinator.main
package sql

import com.socrata.datacoordinator.util.IdProviderPool
import javax.sql.DataSource
import java.sql.Connection

import org.postgresql.copy.CopyManager
import com.rojoma.simplearm.util._
import org.joda.time.DateTime

import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, DatasetInfo, DatasetMapWriter}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresDatasetMapReader, PostgresGlobalLog, PostgresDatasetMapWriter}
import com.socrata.datacoordinator.truth.loader.Logger
import com.socrata.datacoordinator.truth.loader.sql.{RepBasedSchemaLoader, SqlLogger}
import com.socrata.datacoordinator.manifest.sql.SqlTruthManifest

trait PostgresDatabaseMutator[CT, CV] extends DatabaseMutator[CT, CV] { self =>
  def idProviderPool: IdProviderPool
  def postgresDataSource: DataSource
  def copyManagerFromConnection(conn: Connection): CopyManager =
    conn.asInstanceOf[org.postgresql.PGConnection].getCopyAPI
  def physicalColumnBaseForType(typ: CT): String
  def nameForType(typ: CT): String
  def repFor(columnInfo: ColumnInfo): SqlColumnRep[CT, CV]

  final def withTransaction[T]()(f: ProviderOfNecessaryThings => T): T = {
    using(postgresDataSource.getConnection) { conn =>
      conn.setAutoCommit(false)

      val provider = new ProviderOfNecessaryThings {
        val now = DateTime.now()

        val datasetMapWriter = new PostgresDatasetMapWriter(conn)
        val datasetMapReader = new PostgresDatasetMapReader(conn)

        def datasetLog(ds: DatasetInfo): Logger[CV] =
          new SqlLogger[CV](conn, ds.tableBase + "_log", ???)

        def schemaLoader(version: datasetMapWriter.VersionInfo, logger: Logger[CV]) =
          new RepBasedSchemaLoader[CT, CV](conn, logger) {
            def repFor(columnInfo:DatasetMapWriter#ColumnInfo) = self.repFor(columnInfo)
          }

        val globalLog = new PostgresGlobalLog(conn)

        val truthManifest = new SqlTruthManifest(conn)

        val idProviderPool = PostgresDatabaseMutator.this.idProviderPool

        def physicalColumnBaseForType(typ: CT) = self.physicalColumnBaseForType(typ)

        def nameForType(typ: CT) = self.nameForType(typ)
      }

      val result = f(provider)
      conn.commit()
      result
    }
  }
}
