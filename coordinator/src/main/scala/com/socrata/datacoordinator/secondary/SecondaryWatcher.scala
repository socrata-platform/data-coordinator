package com.socrata.datacoordinator.secondary

import com.rojoma.simplearm.Managed
import scala.concurrent.duration._
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.secondary.sql.SqlSecondaryConfig
import com.typesafe.config.{Config, ConfigFactory}
import com.socrata.datacoordinator.common.{DataSourceConfig, SoQLCommon, DataSourceFromConfig}
import java.io.File
import com.socrata.soql.types.{SoQLValue, SoQLType}
import org.slf4j.LoggerFactory
import sun.misc.{Signal, SignalHandler}
import java.util.concurrent.{Executors, TimeUnit, CountDownLatch}
import org.joda.time.{DateTime, Seconds}
import com.socrata.datacoordinator.util.{NullCache, StackedTimingReport, LoggedTimingReport}
import com.socrata.datacoordinator.truth.universe._
import org.apache.log4j.PropertyConfigurator
import com.socrata.thirdparty.typesafeconfig.Propertizer
import scala.concurrent.duration.Duration
import com.socrata.datacoordinator.service.{SecondaryConfig => ServiceSecondaryConfig}
import scala.util.Random
import java.util.UUID
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo

class SecondaryWatcher[CT, CV](universe: => Managed[SecondaryWatcher.UniverseType[CT, CV]], claimantId: UUID, claimTimeout: FiniteDuration) {
  import SecondaryWatcher.log
  private val rand = new Random()
  // splay the sleep time +/- 5s to prevent watchers from getting in lock step
  private val nextRuntimeSplay = (rand.nextInt(10000) - 5000).toLong

  def run(u: Universe[CT, CV] with SecondaryManifestProvider with PlaybackToSecondaryProvider, secondary: NamedSecondary[CT, CV]): Boolean = {
    import u._

    val foundWorkToDo = for (job <-  secondaryManifest.claimDatasetNeedingReplication(secondary.storeId, claimantId, claimTimeout)) yield {
      log.info("Syncing {} into {}", job.datasetId, secondary.storeId)
      try {
        playbackToSecondary(secondary, job)
      } catch {
        case e: Exception =>
          log.error("Unexpected exception while updating dataset {} in secondary {}; marking it as broken", job.datasetId.asInstanceOf[AnyRef], secondary.storeId, e)
          secondaryManifest.markSecondaryDatasetBroken(job)
      } finally {
        try {
          secondaryManifest.releaseClaimedDataset(job)
        } catch {
          case e: Exception =>
            log.error("Unexpected exception while releasing claim on dataset {} in secondary {}", job.datasetId.asInstanceOf[AnyRef], secondary.storeId, e)
        }
      }
    }
    foundWorkToDo.isDefined
  }

  def cleanOphanedJobs(u: Universe[CT, CV] with SecondaryManifestProvider with PlaybackToSecondaryProvider, secondary: NamedSecondary[CT, CV]) {
    import u._
    secondaryManifest.cleanOrphanedClaimedDatasets(secondary.storeId, claimantId)
  }

  private def maybeSleep(storeId: String, nextRunTime: DateTime, finished: CountDownLatch): Boolean = {
    val remainingTime = nextRunTime.getMillis - System.currentTimeMillis() - nextRuntimeSplay
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

  def mainloop(secondaryConfigInfo: SecondaryConfigInfo, secondary: Secondary[CT, CV], finished: CountDownLatch) {
    // Clean up any orphaned jobs which may have been created by this watcher
    // This needs to be done outside the loop such that it's only executed once.  It also requires access to u.secondaryManifest
    for { u <- universe } yield {
      import u.secondaryManifest
      secondaryManifest.cleanOrphanedClaimedDatasets(secondaryConfigInfo.storeId, claimantId)
    }

    var lastWrote = new DateTime(0L)
    var nextRunTime = bestNextRunTime(secondaryConfigInfo.storeId,
      secondaryConfigInfo.nextRunTime,
      secondaryConfigInfo.runIntervalSeconds)

    var done = maybeSleep(secondaryConfigInfo.storeId, nextRunTime, finished)
    while(!done) {
      for { u <- universe } yield {
        import u._

        while(run(u, new NamedSecondary(secondaryConfigInfo.storeId, secondary)) && finished.getCount != 0) {
          // loop until we either have no more work or we are told to exit
        }

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

class SecondaryWatcherConfig(config: Config, root: String) {
  private def k(s: String) = root + "." + s
  val log4j = config.getConfig(k("log4j"))
  val database = new DataSourceConfig(config, k("database"))
  val secondaryConfig = new ServiceSecondaryConfig(config.getConfig(k("secondary")))
  val instance = config.getString(k("instance"))
  val watcherId = UUID.fromString(config.getString(k("watcher-id")))
  val claimTimeout = config.getMilliseconds(k("claim-timeout")).longValue.millis
  val tmpdir = new File(config.getString(k("tmpdir"))).getAbsoluteFile

}

class SecondaryWatcherClaimManager(dsInfo: DSInfo, claimantId: UUID, claimTimeout: FiniteDuration) {
  import SecondaryWatcher.log
  val updateInterval = claimTimeout / 2

  def mainloop(finished: CountDownLatch) {
    var lastUpdate = DateTime.now()

    var done = awaitEither(finished, updateInterval.toMillis)
    while(!done) {
      try {
        updateDatasetClaimedAtTime()
        lastUpdate = DateTime.now()
      } catch {
        case e: Exception =>
          log.error("Unexpected exception while updating claimedAt time for secondary sync jobs claimed by watcherId " + claimantId.toString(), e)
      }
      done = awaitEither(finished, updateInterval.toMillis)
    }
  }

  private def awaitEither(finished: CountDownLatch, interval: Long): Boolean = {
    finished.await(interval, TimeUnit.MILLISECONDS)
  }

  private def updateDatasetClaimedAtTime() {
    using(dsInfo.dataSource.getConnection()) { conn =>
      using(conn.prepareStatement(
        """UPDATE secondary_manifest
        |SET claimed_at = CURRENT_TIMESTAMP
        |WHERE claimant_id = ?""".stripMargin)) { stmt =>
        stmt.setObject(1, claimantId)
        stmt.executeUpdate()
      }
    }
  }
}

object SecondaryWatcher extends App { self =>
  type UniverseType[CT, CV] = Universe[CT, CV] with SecondaryManifestProvider with PlaybackToSecondaryProvider with SecondaryConfigProvider

  val rootConfig = ConfigFactory.load()
  println(rootConfig.root.render())
  val config = new SecondaryWatcherConfig(rootConfig, "com.socrata.coordinator.secondary-watcher")
  PropertyConfigurator.configure(Propertizer("log4j", config.log4j))

  val log = LoggerFactory.getLogger(classOf[SecondaryWatcher[_,_]])

  for(dsInfo <- DataSourceFromConfig(config.database)) {
    val secondaries = SecondaryLoader.load(config.secondaryConfig).asInstanceOf[Map[String, Secondary[SoQLType, SoQLValue]]]

    val executor = Executors.newCachedThreadPool()

    val common = new SoQLCommon(
      dsInfo.dataSource,
      dsInfo.copyIn,
      executor,
      _ => None,
      new LoggedTimingReport(log) with StackedTimingReport,
      allowDdlOnPublishedCopies = false, // don't care,
      Duration.fromNanos(1L), // don't care
      config.instance,
      config.tmpdir,
      Duration.fromNanos(1L), // don't care
      Duration.fromNanos(1L), // don't care
      NullCache
    )

    val w = new SecondaryWatcher(common.universe, config.watcherId, config.claimTimeout)
    val cm = new SecondaryWatcherClaimManager(dsInfo, config.watcherId, config.claimTimeout)

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
        using(dsInfo.dataSource.getConnection()) { conn =>
          val cfg = new SqlSecondaryConfig(conn, common.timingReport)


          new Thread {
              setName("SecondaryWatcher claim time manager")

              override def run() {
                cm.mainloop(finished)
              }
          } :: secondaries.iterator.flatMap { case (name, secondary) =>
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
}
