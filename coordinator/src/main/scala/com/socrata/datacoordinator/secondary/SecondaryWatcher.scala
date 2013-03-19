package com.socrata.datacoordinator.secondary

import scala.concurrent.duration._
import javax.sql.DataSource

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresSecondaryPlaybackManifest, PostgresDatasetMapReader, PostgresGlobalLogPlayback}
import com.socrata.datacoordinator.secondary.sql.{SqlSecondaryConfig, SqlSecondaryManifest}
import com.socrata.datacoordinator.truth.sql.SqlColumnReadRep
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.loader.sql.SqlDelogger
import com.socrata.datacoordinator.truth.RowLogCodec
import com.typesafe.config.ConfigFactory
import com.socrata.datacoordinator.common.DataSourceFromConfig
import java.io.File
import com.socrata.datacoordinator.common.soql.{SoQLRowLogCodec, SoQLRep}
import com.socrata.soql.types.SoQLType
import com.socrata.soql.environment.TypeName
import org.slf4j.LoggerFactory
import sun.misc.{Signal, SignalHandler}
import java.util.concurrent.{TimeUnit, CountDownLatch}
import org.joda.time.{DateTime, Seconds}
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.util.{StackedTimingReport, LoggedTimingReport, TimingReport}

class SecondaryWatcher[CT, CV](ds: DataSource, repFor: ColumnInfo => SqlColumnReadRep[CT, CV], codecFactory: () => RowLogCodec[CV], timingReport: TimingReport) {
  import SecondaryWatcher.log

  def run(secondary: NamedSecondary[CV]) {
    using(ds.getConnection()) { conn =>
      conn.setAutoCommit(false)
      val globalLog = new PostgresGlobalLogPlayback(conn)
      val playbackMfst = new PostgresSecondaryPlaybackManifest(conn, secondary.storeId)
      val secondaryManifest = new SqlSecondaryManifest(conn)
      val pb = new PlaybackToSecondary(conn, secondaryManifest, repFor, timingReport)
      val dsmr = new PostgresDatasetMapReader(conn)
      var lastJobId = playbackMfst.lastJobId()
      val datasetsInStore = secondaryManifest.datasets(secondary.storeId)
      for(job <- globalLog.pendingJobs(lastJobId)) {
        assert(lastJobId.underlying + 1 == job.id.underlying, "Missing backup ID in global log??")
        lastJobId = job.id

        if(datasetsInStore.contains(job.datasetId)) {
          dsmr.datasetInfo(job.datasetId) match {
            case Some(datasetInfo) =>
              log.info("Syncing {} (#{}) into {}", datasetInfo.datasetName, job.datasetId.underlying.asInstanceOf[AnyRef], secondary.storeId)
              val delogger = new SqlDelogger(conn, datasetInfo.logTableName, codecFactory)
              pb(job.datasetId, secondary, dsmr, delogger)
            case None =>
              log.info("Dropping dataset #{} from {}", job.datasetId.underlying, secondary.storeId)
              pb.drop(secondary, job.datasetId)
              secondaryManifest.dropDataset(secondary.storeId, job.datasetId)
          }
        }
        playbackMfst.finishedJob(job.id)
        conn.commit()
      }
    }
  }

  private def maybeSleep(storeId: String, nextRunTime: DateTime, finished: CountDownLatch): Boolean = {
    val remainingTime = nextRunTime.getMillis - System.currentTimeMillis()
    if(remainingTime <= 0) log.warn("{} is behind schedule {}ms", storeId, remainingTime.abs)
    finished.await(remainingTime, TimeUnit.MILLISECONDS)
  }

  def mainloop(secondaryConfigInfo: SecondaryConfigInfo, secondary: Secondary[CV], finished: CountDownLatch) {
    var lastWrote = new DateTime(0L)
    var nextRunTime = secondaryConfigInfo.nextRunTime
    if(nextRunTime.compareTo(DateTime.now()) < 0) nextRunTime = DateTime.now()

    var done = maybeSleep(secondaryConfigInfo.storeId, nextRunTime, finished)
    while(!done) {
      run(new NamedSecondary(secondaryConfigInfo.storeId, secondary))

      nextRunTime = nextRunTime.plus(Seconds.seconds(secondaryConfigInfo.runIntervalSeconds))

      // we only actually write at most once every 15 minutes...
      val now = DateTime.now()
      if(now.getMillis - lastWrote.getMillis >= 15 * 60 * 1000) {
        log.info("Writing new next-runtime")
        using(ds.getConnection()) { conn =>
          conn.setAutoCommit(false)
          val cfg = new SqlSecondaryConfig(conn)
          cfg.updateNextRunTime(secondaryConfigInfo.storeId, nextRunTime)
          conn.commit()
        }
        lastWrote = now
      }

      done = maybeSleep(secondaryConfigInfo.storeId, nextRunTime, finished)
    }
  }
}

object SecondaryWatcher extends App {
  val log = LoggerFactory.getLogger(classOf[SecondaryWatcher[_,_]])

  val timingReport = new LoggedTimingReport(log) with StackedTimingReport

  val rootConfig = ConfigFactory.load()
  val config = rootConfig.getConfig("com.socrata.secondary-watcher")
  println(config.root.render())
  val (dataSource, _) = DataSourceFromConfig(config)
  val secondaries = SecondaryLoader.load(config.getConfig("secondary.configs"), new File(config.getString("secondary.path"))).asInstanceOf[Map[String, Secondary[Any]]]
  def repFor(ci: ColumnInfo) =
    SoQLRep.sqlRepFactories(SoQLType.typesByName(TypeName(ci.typeName)))(ci.physicalColumnBaseBase)

  val pause = config.getMilliseconds("sleep-time").longValue.millis

  val w = new SecondaryWatcher[SoQLType, Any](dataSource, repFor, () => SoQLRowLogCodec, timingReport)

  val SIGTERM = new Signal("TERM")
  val SIGINT = new Signal("INT")

  val finished = new CountDownLatch(1)

  val signalHandler = new SignalHandler {
    val firstSignal = new java.util.concurrent.atomic.AtomicBoolean(true)
    def handle(signal: Signal) {
      log.info("Signalling shutdown")
      finished.countDown()
    }
  }

  var oldSIGTERM: SignalHandler = null
  var oldSIGINT: SignalHandler = null
  try {
    log.info("Hooking SIGTERM and SIGINT")
    oldSIGTERM = Signal.handle(SIGTERM, signalHandler)
    oldSIGINT = Signal.handle(SIGINT, signalHandler)

    val threads =
      using(dataSource.getConnection()) { conn =>
        val cfg = new SqlSecondaryConfig(conn)

        secondaries.iterator.flatMap { case (name, secondary) =>
          cfg.lookup(name).map { info =>
            new Thread {
              setName("Worker for secondary " + name)

              override def run() {
                w.mainloop(info, secondary, finished)
              }
            }
          }.orElse {
            log.warn("Secondary {} is defined, but there is no record in the secondary config table", name)
            None
          }
        }.toList
      }

    threads.foreach(_.start())

    log.info("Going to sleep...")
    finished.await()

    log.info("Waiting for threads to stop...")
    threads.foreach(_.join())
  } finally {
    log.info("Un-hooking SIGTERM and SIGINT")
    if(oldSIGTERM != null) Signal.handle(SIGTERM, oldSIGTERM)
    if(oldSIGTERM != null) Signal.handle(SIGINT, oldSIGINT)
  }

  secondaries.values.foreach(_.shutdown())
}
