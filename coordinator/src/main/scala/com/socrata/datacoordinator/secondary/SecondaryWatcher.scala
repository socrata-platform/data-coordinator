package com.socrata.datacoordinator.secondary

import java.util.UUID
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import com.rojoma.simplearm.Managed
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.common.{DataSourceFromConfig, SoQLCommon}
import com.socrata.datacoordinator.secondary.sql.SqlSecondaryInfo
import com.socrata.datacoordinator.truth.universe._
import com.socrata.datacoordinator.util._
import com.socrata.http.server.util.RequestId
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.thirdparty.metrics.{MetricsOptions, MetricsReporter}
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.typesafe.config.ConfigFactory
import org.apache.log4j.PropertyConfigurator
import org.joda.time.{DateTime, Seconds}
import org.slf4j.LoggerFactory
import sun.misc.{Signal, SignalHandler}

import scala.concurrent.duration._
import scala.util.Random

class SecondaryWatcher[CT, CV](universe: => Managed[SecondaryWatcher.UniverseType[CT, CV]],
                               claimantId: UUID,
                               claimTimeout: FiniteDuration,
                               backoffInterval: FiniteDuration,
                               maxRetries: Int,
                               timingReport: TimingReport) {
  val log = LoggerFactory.getLogger(classOf[SecondaryWatcher[_,_]])
  private val rand = new Random()
  // splay the sleep time +/- 5s to prevent watchers from getting in lock step
  private val nextRuntimeSplay = (rand.nextInt(10000) - 5000).toLong

  // allow for overriding for easy testing
  protected def manifest(u: Universe[CT, CV] with SecondaryManifestProvider with PlaybackToSecondaryProvider):
      SecondaryManifest = u.secondaryManifest

  def run(u: Universe[CT, CV] with SecondaryManifestProvider with PlaybackToSecondaryProvider,
          secondary: NamedSecondary[CT, CV]): Boolean = {
    import u._

    val foundWorkToDo = for (job <- manifest(u).claimDatasetNeedingReplication(
                                      secondary.storeId, claimantId, claimTimeout)) yield {
      timingReport(
        "playback-to-secondary",
        "tag:job-id" -> RequestId.generate(), // add job id tag to enclosing logs
        "tag:dataset-id" -> job.datasetId, // add dataset id tag to enclosing logs
        "truthDatasetId" -> job.datasetId.underlying,
        "secondary" -> secondary.storeId,
        "endingDataVersion" -> job.endingDataVersion
      ) {
        log.info(">> Syncing {} into {}", job.datasetId, secondary.storeId)
        if (job.retryNum > 0) log.info("Retry #{} of {}", job.retryNum, maxRetries)
        try {
          playbackToSecondary(secondary, job)
          log.info("<< Sync done for {} into {}", job.datasetId, secondary.storeId)
        } catch {
          case e: Exception =>
            if (job.retryNum < maxRetries) {
              val retryBackoff = backoffInterval.toSeconds * Math.pow(2, job.retryNum)
              log.warn("Unexpected exception while updating dataset {} in secondary {}, retrying in {}...",
                       job.datasetId.asInstanceOf[AnyRef], secondary.storeId, retryBackoff.toString, e)
              manifest(u).updateRetryInfo(secondary.storeId, job.datasetId, job.retryNum + 1,
                                             retryBackoff.toInt)
            } else {
              log.error("Unexpected exception while updating dataset {} in secondary {}; marking it as broken",
                        job.datasetId.asInstanceOf[AnyRef], secondary.storeId, e)
              manifest(u).markSecondaryDatasetBroken(job)
            }
        } finally {
          try {
            manifest(u).releaseClaimedDataset(job)
          } catch {
            case e: Exception =>
              log.error("Unexpected exception while releasing claim on dataset {} in secondary {}",
                        job.datasetId.asInstanceOf[AnyRef], secondary.storeId, e)
          }
        }
      }
    }
    foundWorkToDo.isDefined
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
      log.warn("{} is behind schedule {}s ({} intervals)",
               storeId, remainingTime.asInstanceOf[AnyRef], diffInIntervals.asInstanceOf[AnyRef])
      val newTarget = target.plus(Seconds.seconds(Math.min(diffInIntervals * interval, Int.MaxValue).toInt))
      log.warn("Resetting target time to {}", newTarget)
      newTarget
    } else {
      target
    }
  }

  /**
   * Cleanup any orphaned jobs created by this watcher exiting uncleanly.  Needs to be run
   * without any workers running (ie. at startup).
   */
  private def cleanOrphanedJobs(instance: SecondaryInstanceInfo): Unit = {
    // At lean up any orphaned jobs which may have been created by this watcher
    for { u <- universe } yield {
      u.secondaryManifest.cleanOrphanedClaimedDatasets(instance.storeId, claimantId)
    }
  }

  def mainloop(instance: SecondaryInstanceInfo, secondary: Secondary[CT, CV], finished: CountDownLatch) {
    var lastWrote = new DateTime(0L)
    var nextRunTime = bestNextRunTime(instance.storeId,
      instance.nextRunTime,
      instance.runIntervalSeconds)

    var done = maybeSleep(instance.storeId, nextRunTime, finished)
    while (!done) {
      try {
        for {u <- universe} yield {
          import u._

          while (run(u, new NamedSecondary(instance.storeId, secondary)) && finished.getCount != 0) {
            // loop until we either have no more work or we are told to exit
          }

          nextRunTime = bestNextRunTime(instance.storeId,
            nextRunTime.plus(Seconds.seconds(instance.runIntervalSeconds)),
            instance.runIntervalSeconds)

          // we only actually write at most once every 15 minutes...
          val now = DateTime.now()
          if (now.getMillis - lastWrote.getMillis >= 15 * 60 * 1000) {
            log.info("Writing new next-runtime: {}", nextRunTime)
            secondaryInfo.updateNextRunTime(instance.storeId, nextRunTime)
          }
          lastWrote = now
        }

        done = maybeSleep(instance.storeId, nextRunTime, finished)
      } catch {
        case e: Exception =>
          log.error("Unexpected exception while updating claimedAt time for secondary sync jobs claimed by watcherId " + claimantId.toString(), e)
      }
    }
  }
}

class SecondaryWatcherClaimManager(dsInfo: DSInfo, claimantId: UUID, claimTimeout: FiniteDuration) {
  import com.socrata.datacoordinator.secondary.SecondaryWatcher.log
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
          log.error("Unexpected exception while updating claimedAt time for secondary sync jobs " +
                    "claimed by watcherId " + claimantId.toString(), e)
      }
      done = awaitEither(finished, updateInterval.toMillis)
    }
  }

  private def awaitEither(finished: CountDownLatch, interval: Long): Boolean = {
    finished.await(interval, TimeUnit.MILLISECONDS)
  }

  private def updateDatasetClaimedAtTime() {
    // TODO: We have a difficult to debug problem here if our connection pool isn't large enough to satisfy
    // all our workers, since it can block claim updates and result in claims being overwritten.
    // For now we are working around it by configuring a large pool and relying on worker config to control
    // concurrency.  We could potentially have a separate pool for the claim manager.
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
  type UniverseType[CT, CV] = Universe[CT, CV] with SecondaryManifestProvider with
                                                    PlaybackToSecondaryProvider with
                                                    SecondaryInfoProvider

  val rootConfig = ConfigFactory.load()
  val config = new SecondaryWatcherConfig(rootConfig, "com.socrata.coordinator.secondary-watcher")
  PropertyConfigurator.configure(Propertizer("log4j", config.log4j))

  val log = LoggerFactory.getLogger(classOf[SecondaryWatcher[_,_]])
  log.info("Starting secondary watcher...")

  val metricsOptions = MetricsOptions(config.metrics)

  Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
    def uncaughtException(t: Thread, e: Throwable) {
      log.error(s"Uncaught exception in thread ${t.getName}, exiting", e)
      sys.exit(1)
    }
  })

  for { dsInfo <- DataSourceFromConfig(config.database)
        reporter <- MetricsReporter.managed(metricsOptions) } {
    val secondaries = SecondaryLoader.load(config.secondaryConfig).
                        asInstanceOf[Map[String, Secondary[SoQLType, SoQLValue]]]

    val executor = Executors.newCachedThreadPool()

    val common = new SoQLCommon(
      dsInfo.dataSource,
      dsInfo.copyIn,
      executor,
      _ => None,
      new LoggedTimingReport(log) with StackedTimingReport with MetricsTimingReport with TaggableTimingReport,
      allowDdlOnPublishedCopies = false, // don't care,
      Duration.fromNanos(1L), // don't care
      config.instance,
      config.tmpdir,
      Duration.fromNanos(1L), // don't care
      Duration.fromNanos(1L), // don't care
      NullCache
    )

    val w = new SecondaryWatcher(common.universe, config.watcherId, config.claimTimeout,
                                 config.backoffInterval, config.maxRetries, common.timingReport)
    val cm = new SecondaryWatcherClaimManager(dsInfo, config.watcherId, config.claimTimeout)

    val SIGTERM = new Signal("TERM")
    val SIGINT = new Signal("INT")

    /** Flags when we want to start shutting down, don't process new work */
    val initiateShutdown = new CountDownLatch(1)
    /** Flags when we have stopped processing work and are ready to actually shutdown */
    val completeShutdown = new CountDownLatch(1)

    val signalHandler = new SignalHandler {
      val firstSignal = new java.util.concurrent.atomic.AtomicBoolean(true)
      def handle(signal: Signal) {
        log.info("Signalling shutdown")
        initiateShutdown.countDown()
      }
    }

    var oldSIGTERM: SignalHandler = null
    var oldSIGINT: SignalHandler = null
    try {
      log.info("Hooking SIGTERM and SIGINT")
      oldSIGTERM = Signal.handle(SIGTERM, signalHandler)
      oldSIGINT = Signal.handle(SIGINT, signalHandler)

      val claimTimeManagerThread = new Thread {
        setName("SecondaryWatcher claim time manager")

        override def run() {
          cm.mainloop(completeShutdown)
        }
      }

      val workerThreads = for { u <- common.universe } yield {
        secondaries.iterator.flatMap {
          case (name, secondary) =>
            u.secondaryInfo.instance(name).map { info =>
              w.cleanOrphanedJobs(info)

              1 to config.secondaryConfig.instances(name).numWorkers map { n =>
                new Thread {
                  setName(s"Worker $n for secondary $name")

                  override def run() {
                    w.mainloop(info, secondary, initiateShutdown)
                  }
                }
              }
            }.orElse {
              log.warn("Secondary {} is defined, but there is no record in the secondary config table", name)
              None
            }
        }.toList.flatten
      }

      claimTimeManagerThread.start()
      workerThreads.foreach(_.start())

      log.info("Going to sleep...")
      initiateShutdown.await()

      log.info("Waiting for worker threads to stop...")
      workerThreads.foreach(_.join())

      // Can't shutdown claim time manager until workers stop or their jobs might be stolen
      log.info("Shutting down claim time manager...")
      completeShutdown.countDown()
      claimTimeManagerThread.join()
    } finally {
      log.info("Un-hooking SIGTERM and SIGINT")
      if(oldSIGTERM != null) Signal.handle(SIGTERM, oldSIGTERM)
      if(oldSIGTERM != null) Signal.handle(SIGINT, oldSIGINT)
    }

    secondaries.values.foreach(_.shutdown())
    executor.shutdown()
  }
}
