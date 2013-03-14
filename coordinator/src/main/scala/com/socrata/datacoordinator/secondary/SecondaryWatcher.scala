package com.socrata.datacoordinator.secondary

import scala.concurrent.duration._
import javax.sql.DataSource

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresDatasetMapReader, PostgresGlobalLogPlayback}
import com.socrata.datacoordinator.secondary.sql.SqlSecondaryManifest
import com.socrata.datacoordinator.truth.sql.SqlColumnReadRep
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.loader.sql.SqlDelogger
import com.socrata.datacoordinator.truth.RowLogCodec
import scala.concurrent.duration.Duration
import com.typesafe.config.ConfigFactory
import com.socrata.datacoordinator.common.DataSourceFromConfig
import java.io.File
import com.socrata.datacoordinator.common.soql.{SoQLRowLogCodec, SoQLRep}
import com.socrata.soql.types.SoQLType
import com.socrata.soql.environment.TypeName

class SecondaryWatcher[CT, CV](ds: DataSource, secondary: NamedSecondary[CV], repFor: ColumnInfo => SqlColumnReadRep[CT, CV], codecFactory: () => RowLogCodec[CV], pause: Duration) {
  def run() {
    while(true) {
      using(ds.getConnection()) { conn =>
        conn.setAutoCommit(false)
        val globalLog = new PostgresGlobalLogPlayback(conn, forBackup = false)
        val secondaryManifest = new SqlSecondaryManifest(conn)
        val pb = new PlaybackToSecondary(conn, secondaryManifest, repFor)
        val dsmr = new PostgresDatasetMapReader(conn)
        for(job <- globalLog.pendingJobs()) {
          dsmr.datasetInfo(job.datasetId) match {
            case Some(datasetInfo) =>
              val delogger = new SqlDelogger(conn, datasetInfo.logTableName, codecFactory)
              pb(job.datasetId, secondary, dsmr, delogger)
            case None =>
              pb.drop(secondary, job.datasetId)
          }
          globalLog.finishedJob(job)
          conn.commit()
        }
      }
      Thread.sleep(pause.toMillis)
    }
  }
}

object SecondaryWatcher extends App {
  val config = ConfigFactory.load().getConfig("com.socrata.secondary-watcher")
  val (dataSource, _) = DataSourceFromConfig(config)
  val secondaries = SecondaryLoader.load(config.getConfig("secondary.configs"), new File(config.getString("secondary.path")))
  def repFor(ci: ColumnInfo) =
    SoQLRep.sqlRepFactories(SoQLType.typesByName(TypeName(ci.typeName)))(ci.physicalColumnBaseBase)

  val w = new SecondaryWatcher[SoQLType, Any](dataSource, NamedSecondary(secondaries.head._1, secondaries.head._2.asInstanceOf[Secondary[Any]]), repFor, () => SoQLRowLogCodec, 1.second)
}
