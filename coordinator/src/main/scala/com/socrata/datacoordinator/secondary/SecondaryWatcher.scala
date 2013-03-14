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
import org.slf4j.LoggerFactory

class SecondaryWatcher[CT, CV](ds: DataSource, secondaries: Map[String, Secondary[CV]], repFor: ColumnInfo => SqlColumnReadRep[CT, CV], codecFactory: () => RowLogCodec[CV], pause: Duration) {
  val log = LoggerFactory.getLogger(classOf[SecondaryWatcher[CT,CV]])
  def run() {
    while(true) {
      log.trace("Tick")
      using(ds.getConnection()) { conn =>
        conn.setAutoCommit(false)
        val globalLog = new PostgresGlobalLogPlayback(conn, forBackup = false)
        val secondaryManifest = new SqlSecondaryManifest(conn)
        val pb = new PlaybackToSecondary(conn, secondaryManifest, repFor)
        val dsmr = new PostgresDatasetMapReader(conn)
        for(job <- globalLog.pendingJobs()) {
          dsmr.datasetInfo(job.datasetId) match {
            case Some(datasetInfo) =>
              for {
                store <- secondaryManifest.stores(job.datasetId).keys
                secondary <- secondaries.get(store)
              } {
                log.info("Syncing {} (#{}) into {}", Array[AnyRef](datasetInfo.datasetName, job.datasetId.underlying.asInstanceOf[AnyRef], store))
                val delogger = new SqlDelogger(conn, datasetInfo.logTableName, codecFactory)
                pb(job.datasetId, NamedSecondary(store, secondary), dsmr, delogger)
              }
            case None =>
              for {
                store <- secondaryManifest.stores(job.datasetId).keys
                secondary <- secondaries.get(store)
              } {
                log.info("Dropping dataset #{} from {}", job.datasetId.underlying, store)
                pb.drop(NamedSecondary(store, secondary), job.datasetId)
              }
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
  val secondaries = SecondaryLoader.load(config.getConfig("secondary.configs"), new File(config.getString("secondary.path"))).asInstanceOf[Map[String, Secondary[Any]]]
  def repFor(ci: ColumnInfo) =
    SoQLRep.sqlRepFactories(SoQLType.typesByName(TypeName(ci.typeName)))(ci.physicalColumnBaseBase)

  val w = new SecondaryWatcher[SoQLType, Any](dataSource, secondaries, repFor, () => SoQLRowLogCodec, 1.second)

  w.run()
}
