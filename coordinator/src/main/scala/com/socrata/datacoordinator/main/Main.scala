package com.socrata.datacoordinator.main

import com.socrata.datacoordinator.truth.loader.sql.SqlLogger
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, GlobalLog, DatasetMapWriter}
import com.socrata.datacoordinator.truth.loader.Logger
import com.socrata.datacoordinator.manifest.TruthManifest
import org.joda.time.DateTime
import javax.sql.DataSource
import java.sql.Connection
import org.postgresql.copy.CopyManager
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresGlobalLog, PostgresDatasetMapWriter}
import com.socrata.datacoordinator.manifest.sql.SqlTruthManifest
import com.socrata.datacoordinator.util.IdProviderPool

abstract class DatabaseMutator[CT, CV] {
  trait ProviderOfNecessaryThings {
    val now: DateTime
    val datasetMapWriter: DatasetMapWriter
    def datasetLog(ds: datasetMapWriter.DatasetInfo): Logger[CV]
    val globalLog: GlobalLog
    val truthManifest: TruthManifest
    val idProviderPool: IdProviderPool
  }

  def nameForType(typ: CT): String

  def withTransaction[T]()(f: ProviderOfNecessaryThings => T): T
}

trait PostgresDatabaseMutator[CT, CV] extends DatabaseMutator[CT, CV] {
  def idProviderPool: IdProviderPool
  def postgresDataSource: DataSource
  def copyManagerFromConnection(conn: Connection): CopyManager =
    conn.asInstanceOf[org.postgresql.PGConnection].getCopyAPI

  final def withTransaction[T]()(f: ProviderOfNecessaryThings => T): T = {
    using(postgresDataSource.getConnection) { conn =>
      conn.setAutoCommit(false)

      def ??? = sys.error("NYI")

      val provider = new ProviderOfNecessaryThings {
        val now = DateTime.now()

        val datasetMapWriter = new PostgresDatasetMapWriter(conn)

        def datasetLog(ds: datasetMapWriter.DatasetInfo): Logger[CV] =
          new SqlLogger[CV](conn, ds.tableBase + "_log", ???)

        val globalLog = new PostgresGlobalLog(conn)

        val truthManifest = new SqlTruthManifest(conn)

        val idProviderPool = PostgresDatabaseMutator.this.idProviderPool
      }

      val result = f(provider)
      conn.commit()
      result
    }
  }
}

abstract class ColumnAdder[CT, CV] extends DatabaseMutator[CT, CV] {
  // Glue points we want/need
  //
  // Data updates (schema changes, upsert, etc)
  // Global log listener (specifically: a playback in some postgres table)
  // A secondary store (just a dummy for plugging in)
  // Store-update operations
  //  * Refresh dataset X to {StoreSet} : Future[Either[Error, NewVersion]]
  //  * Remove dataset X from {StoreSet} : Future[Option[Error]]
  // Get replication status : Map[Store, Version] (from secondary manifest)

  def addToSchema(dataset: String, columnName: String, columnType: CT) {
    withTransaction() { providerOfNecessaryThings =>
      import providerOfNecessaryThings._
      val ds = datasetMapWriter.datasetInfo(dataset).getOrElse(sys.error("Augh no such dataset"))
      val table = datasetMapWriter.latest(ds)
      val col = datasetMapWriter.addColumn(table, columnName, nameForType(columnType), "c" + idProviderPool.withProvider(_.allocate()))
      val logger = datasetLog(ds)

      sys.error("Still need to implement this bit!")
      // addColumnsToTable(logger, col)

      logger.endTransaction().foreach { ver =>
        truthManifest.updateLatestVersion(ds, ver)
        globalLog.log(ds, ver, now, "who")
      }
    }
  }
}
