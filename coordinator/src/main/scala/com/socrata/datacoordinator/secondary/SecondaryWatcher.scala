package com.socrata.datacoordinator.secondary

import com.rojoma.simplearm.Managed

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
import com.socrata.datacoordinator.util.{StackedTimingReport, LoggedTimingReport}
import com.socrata.datacoordinator.truth.universe._
import org.apache.log4j.PropertyConfigurator
import com.socrata.thirdparty.typesafeconfig.Propertizer
import scala.concurrent.duration.Duration

class SecondaryWatcher[CT, CV](universe: => Managed[SecondaryWatcher.UniverseType[CT, CV]]) {
  import SecondaryWatcher.log

  def run(u: Universe[CT, CV] with SecondaryManifestProvider with PlaybackToSecondaryProvider, secondary: NamedSecondary[CT, CV]) {
    import u._

    val jobs = secondaryManifest.findDatasetsNeedingReplication(secondary.storeId)
    for(job <- jobs) {
      log.info("Syncing {} into {}", job.datasetId, secondary.storeId)
      playbackToSecondary(secondary, job)
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

  def mainloop(secondaryConfigInfo: SecondaryConfigInfo, secondary: Secondary[CT, CV], finished: CountDownLatch) {
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

class SecondaryWatcherConfig(config: Config, root: String) {
  private def k(s: String) = root + "." + s
  val log4j = config.getConfig(k("log4j"))
  val database = new DataSourceConfig(config, k("database"))
  val secondaryConfigs = config.getObject(k("secondary.configs"))
  val secondaryPath = new File(config.getString(k("secondary.path")))
  val instance = config.getString(k("instance"))
  val tmpdir = new File(config.getString(k("tmpdir"))).getAbsoluteFile
}

object SecondaryWatcher extends App { self =>
  type UniverseType[CT, CV] = Universe[CT, CV] with SecondaryManifestProvider with PlaybackToSecondaryProvider with SecondaryConfigProvider

  val rootConfig = ConfigFactory.load()
  println(rootConfig.root.render())
  val config = new SecondaryWatcherConfig(rootConfig, "com.socrata.coordinator.secondary-watcher")
  PropertyConfigurator.configure(Propertizer("log4j", config.log4j))

  val log = LoggerFactory.getLogger(classOf[SecondaryWatcher[_,_]])

  val (dataSource, copyIn) = DataSourceFromConfig(config.database)
  val secondaries = SecondaryLoader.load(config.secondaryConfigs, config.secondaryPath).asInstanceOf[Map[String, Secondary[SoQLType, SoQLValue]]]

  val executor = Executors.newCachedThreadPool()

  val common = new SoQLCommon(
    dataSource,
    copyIn,
    executor,
    _ => None,
    new LoggedTimingReport(log) with StackedTimingReport,
    allowDdlOnPublishedCopies = false, // don't care,
    Duration.fromNanos(1L), // don't care
    config.instance,
    config.tmpdir
  )

  val w = new SecondaryWatcher(common.universe)

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
        val cfg = new SqlSecondaryConfig(conn, common.timingReport)

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
