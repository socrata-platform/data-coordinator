package com.socrata.datacoordinator.secondary

import scala.concurrent.duration._
import com.rojoma.simplearm.{SimpleArm, Managed}

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.secondary.sql.SqlSecondaryConfig
import com.typesafe.config.ConfigFactory
import com.socrata.datacoordinator.common.{StandardObfuscationKeyGenerator, DataSourceFromConfig}
import java.io.File
import com.socrata.soql.types.{SoQLValue, SoQLType}
import org.slf4j.LoggerFactory
import sun.misc.{Signal, SignalHandler}
import java.util.concurrent.{Executors, TimeUnit, CountDownLatch}
import org.joda.time.{DateTime, Seconds}
import com.socrata.datacoordinator.util.{TransferrableContextTimingReport, StackedTimingReport, LoggedTimingReport, TimingReport}
import com.socrata.datacoordinator.truth.universe._
import java.sql.Connection
import com.socrata.datacoordinator.truth.universe.sql.{PostgresCopyIn, PostgresUniverse}
import com.socrata.datacoordinator.common.soql.universe.PostgresUniverseCommonSupport
import com.socrata.datacoordinator.id.RowId

class SecondaryWatcher[CT, CV](universe: => Managed[SecondaryWatcher.UniverseType[CT, CV]]) {
  import SecondaryWatcher.log

  def run(u: Universe[CT, CV] with DatasetMapReaderProvider with GlobalLogPlaybackProvider with SecondaryManifestProvider with SecondaryPlaybackManifestProvider with PlaybackToSecondaryProvider with DeloggerProvider, secondary: NamedSecondary[CV]) {
    import u._

    val globalLog = globalLogPlayback
    val playbackMfst = secondaryPlaybackManifest(secondary.storeId)
    val pb = playbackToSecondary
    val dsmr = datasetMapReader
    var lastJobId = playbackMfst.lastJobId()
    val datasetsInStore = secondaryManifest.datasets(secondary.storeId)
    for(job <- globalLog.pendingJobs(lastJobId)) {
      assert(lastJobId.underlying + 1 == job.id.underlying, "Missing backup ID in global log??")
      lastJobId = job.id

      if(datasetsInStore.contains(job.datasetId)) {
        dsmr.datasetInfo(job.datasetId) match {
          case Some(datasetInfo) =>
            log.info("Syncing {} (#{}) into {}", datasetInfo.datasetName, job.datasetId.underlying.asInstanceOf[AnyRef], secondary.storeId)
            pb(job.datasetId, secondary, dsmr, delogger(datasetInfo))
          case None =>
            log.info("Dropping dataset #{} from {}", job.datasetId.underlying, secondary.storeId)
            pb.drop(secondary, job.datasetId)
            secondaryManifest.dropDataset(secondary.storeId, job.datasetId)
        }
      }
      playbackMfst.finishedJob(job.id)
      commit()
    }
  }

  private def maybeSleep(storeId: String, nextRunTime: DateTime, finished: CountDownLatch): Boolean = {
    val remainingTime = nextRunTime.getMillis - System.currentTimeMillis()
    finished.await(remainingTime, TimeUnit.MILLISECONDS)
  }

  def bestNextRunTime(storeId: String, target: DateTime, interval: Int): DateTime = {
    val now = DateTime.now()
    val remainingTime = (now.getMillis - target.getMillis) / 1000
    if(remainingTime > 0) {
      val diffInIntervals = remainingTime / interval
      log.warn("{} is behind schedule {}s ({} intervals)", storeId, remainingTime.asInstanceOf[AnyRef], diffInIntervals.asInstanceOf[AnyRef])
      val newTarget = target.plus(Seconds.seconds(Math.min(diffInIntervals * interval, Int.MaxValue).toInt))
      log.warn("Resetting target time to {}", newTarget)
      newTarget
    } else {
      target
    }
  }

  def mainloop(secondaryConfigInfo: SecondaryConfigInfo, secondary: Secondary[CV], finished: CountDownLatch) {
    var lastWrote = new DateTime(0L)
    var nextRunTime = bestNextRunTime(secondaryConfigInfo.storeId,
      secondaryConfigInfo.nextRunTime,
      secondaryConfigInfo.runIntervalSeconds)

    var done = maybeSleep(secondaryConfigInfo.storeId, nextRunTime, finished)
    while(!done) {
      for { u <- universe } yield {
        import u._

        run(u, new NamedSecondary(secondaryConfigInfo.storeId, secondary))


        nextRunTime = bestNextRunTime(secondaryConfigInfo.storeId,
          nextRunTime.plus(Seconds.seconds(secondaryConfigInfo.runIntervalSeconds)),
          secondaryConfigInfo.runIntervalSeconds)

        // we only actually write at most once every 15 minutes...
        val now = DateTime.now()
        if(now.getMillis - lastWrote.getMillis >= 15 * 60 * 1000) {
          log.info("Writing new next-runtime: {}", nextRunTime)
          secondaryConfig.updateNextRunTime(secondaryConfigInfo.storeId, nextRunTime)
        }
        lastWrote = now
      }

      done = maybeSleep(secondaryConfigInfo.storeId, nextRunTime, finished)
    }
  }
}

object SecondaryWatcher extends App { self =>
  val log = LoggerFactory.getLogger(classOf[SecondaryWatcher[_,_]])

  type UniverseType[CT, CV] = Universe[CT, CV] with DatasetMapReaderProvider with GlobalLogPlaybackProvider with SecondaryManifestProvider with SecondaryPlaybackManifestProvider with PlaybackToSecondaryProvider with DeloggerProvider with SecondaryConfigProvider

  val timingReport = new LoggedTimingReport(log) with StackedTimingReport

  val rootConfig = ConfigFactory.load()
  val config = rootConfig.getConfig("com.socrata.secondary-watcher")
  println(config.root.render())
  val (dataSource, _) = DataSourceFromConfig(config)
  val secondaries = SecondaryLoader.load(config.getConfig("secondary.configs"), new File(config.getString("secondary.path"))).asInstanceOf[Map[String, Secondary[SoQLValue]]]

  val executor = Executors.newCachedThreadPool()

  val commonSupport = new PostgresUniverseCommonSupport(executor, _ => None, PostgresCopyIn, StandardObfuscationKeyGenerator, new RowId(0L))

  type SoQLUniverse = PostgresUniverse[SoQLType, SoQLValue]
  def soqlUniverse(conn: Connection, timingReport: TransferrableContextTimingReport) =
    new SoQLUniverse(conn, commonSupport, timingReport, ":secondary-watcher")

  val universe = new SimpleArm[SoQLUniverse] {
    def flatMap[B](f: SoQLUniverse => B): B = {
      using(dataSource.getConnection()) { conn =>
        conn.setAutoCommit(false)
        val result = f(soqlUniverse(conn, timingReport))
        conn.commit()
        result
      }
    }
  }

  val w = new SecondaryWatcher(universe)

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
        val cfg = new SqlSecondaryConfig(conn, timingReport)

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
  executor.shutdown()
}
