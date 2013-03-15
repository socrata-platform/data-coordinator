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
import sun.misc.{Signal, SignalHandler}
import java.util.concurrent.atomic.AtomicBoolean

class SecondaryWatcher[CT, CV](ds: DataSource, secondaries: Map[String, Secondary[CV]], repFor: ColumnInfo => SqlColumnReadRep[CT, CV], codecFactory: () => RowLogCodec[CV]) {
  import SecondaryWatcher.log

  def run() {
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
  }
}

object SecondaryWatcher extends App {
  val log = LoggerFactory.getLogger(classOf[SecondaryWatcher[_,_]])

  val rootConfig = ConfigFactory.load()
  val config = rootConfig.getConfig("com.socrata.secondary-watcher")
  println(config.root.render())
  val (dataSource, _) = DataSourceFromConfig(config)
  val secondaries = SecondaryLoader.load(config.getConfig("secondary.configs"), new File(config.getString("secondary.path"))).asInstanceOf[Map[String, Secondary[Any]]]
  def repFor(ci: ColumnInfo) =
    SoQLRep.sqlRepFactories(SoQLType.typesByName(TypeName(ci.typeName)))(ci.physicalColumnBaseBase)

  val pause = config.getMilliseconds("sleep-time").longValue.millis

  val w = new SecondaryWatcher[SoQLType, Any](dataSource, secondaries, repFor, () => SoQLRowLogCodec)

  val SIGTERM = new Signal("TERM")
  val SIGINT = new Signal("INT")

  val signalled = new AtomicBoolean(false)

  val signalHandler = new SignalHandler {
    val firstSignal = new java.util.concurrent.atomic.AtomicBoolean(true)
    def handle(signal: Signal) {
      if(signalled.getAndSet(true)) log.info("Shutdown already in progress")
      else log.info("Signalling main thread to shut down")
    }
  }

  var oldSIGTERM: SignalHandler = null
  var oldSIGINT: SignalHandler = null
  try {
    log.info("Hooking SIGTERM and SIGINT")
    oldSIGTERM = Signal.handle(SIGTERM, signalHandler)
    oldSIGINT = Signal.handle(SIGINT, signalHandler)

    while(!signalled.get()) {
      log.trace("Tick")
      w.run()
      Thread.sleep(pause.toMillis)
    }
  } finally {
    log.info("Un-hooking SIGTERM and SIGINT")
    if(oldSIGTERM != null) Signal.handle(SIGTERM, oldSIGTERM)
    if(oldSIGTERM != null) Signal.handle(SIGINT, oldSIGINT)
  }

  secondaries.values.foreach(_.shutdown())
}
