package com.socrata.datacoordinator.main

import com.socrata.datacoordinator.truth.loader.sql.{RepBasedSchemaLoader, SqlLogger}
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, GlobalLog, DatasetMapWriter}
import com.socrata.datacoordinator.truth.loader.{SchemaLoader, Logger}
import com.socrata.datacoordinator.manifest.TruthManifest
import org.joda.time.DateTime
import javax.sql.DataSource
import java.sql.Connection
import org.postgresql.copy.CopyManager
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresGlobalLog, PostgresDatasetMapWriter}
import com.socrata.datacoordinator.manifest.sql.SqlTruthManifest
import com.socrata.datacoordinator.util.IdProviderPool
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.sql.SqlColumnRep

abstract class DatabaseMutator[CT, CV] {
  trait ProviderOfNecessaryThings {
    val now: DateTime
    val datasetMapWriter: DatasetMapWriter
    def datasetLog(ds: datasetMapWriter.DatasetInfo): Logger[CV]
    val globalLog: GlobalLog
    val truthManifest: TruthManifest
    val idProviderPool: IdProviderPool
    def physicalColumnBaseForType(typ: CT): String
    def loader(version: datasetMapWriter.VersionInfo): SchemaLoader
    def nameForType(typ: CT): String

    def singleId() = {
      val provider = idProviderPool.borrow()
      try {
        provider.allocate()
      } finally {
        idProviderPool.release(provider)
      }
    }
  }

  def withTransaction[T]()(f: ProviderOfNecessaryThings => T): T
}

trait PostgresDatabaseMutator[CT, CV] extends DatabaseMutator[CT, CV] { self =>
  def idProviderPool: IdProviderPool
  def postgresDataSource: DataSource
  def copyManagerFromConnection(conn: Connection): CopyManager =
    conn.asInstanceOf[org.postgresql.PGConnection].getCopyAPI
  def physicalColumnBaseForType(typ: CT): String
  def nameForType(typ: CT): String
  def repFor(columnInfo: DatasetMapWriter#ColumnInfo): SqlColumnRep[CT, CV]

  final def withTransaction[T]()(f: ProviderOfNecessaryThings => T): T = {
    using(postgresDataSource.getConnection) { conn =>
      conn.setAutoCommit(false)

      val provider = new ProviderOfNecessaryThings {
        val now = DateTime.now()

        val datasetMapWriter = new PostgresDatasetMapWriter(conn)

        def datasetLog(ds: datasetMapWriter.DatasetInfo): Logger[CV] =
          new SqlLogger[CV](conn, ds.tableBase + "_log", ???)

        def loader(version: datasetMapWriter.VersionInfo) =
          new RepBasedSchemaLoader[CT, CV](conn, version.datasetInfo.tableBase + "_" + version.lifecycleVersion) {
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
