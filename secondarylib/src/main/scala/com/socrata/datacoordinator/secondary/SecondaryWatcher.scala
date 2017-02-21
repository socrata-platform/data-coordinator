package com.socrata.datacoordinator.secondary

import java.lang.Runnable
import java.util.UUID
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit, ScheduledExecutorService}

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.secondary.config.SecondaryConfig

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Random

import com.rojoma.simplearm.Managed
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.common.{DataSourceFromConfig, SoQLCommon}
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.secondary.sql.SqlSecondaryStoresConfig
import com.socrata.datacoordinator.truth.universe._
import com.socrata.datacoordinator.util._
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.thirdparty.metrics.{MetricsOptions, MetricsReporter}
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.PropertyConfigurator
import org.joda.time.{DateTime, Seconds}
import org.slf4j.LoggerFactory
import sun.misc.{Signal, SignalHandler}

class SecondaryWatcher[CT, CV](universe: => Managed[SecondaryWatcher.UniverseType[CT, CV]],
                               claimantId: UUID,
                               claimTimeout: FiniteDuration,
                               backoffInterval: FiniteDuration,
                               replayWait: FiniteDuration,
                               maxReplayWait: FiniteDuration,
                               maxRetries: Int,
                               maxReplays: Int,
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

    val foundWorkToDo = for(job <- manifest(u).claimDatasetNeedingReplication(
                                      secondary.storeId, claimantId, claimTimeout)) yield {
      timingReport(
        "playback-to-secondary",
        "tag:job-id" -> UUID.randomUUID(), // add job id tag to enclosing logs
        "tag:dataset-id" -> job.datasetId, // add dataset id tag to enclosing logs
        "truthDatasetId" -> job.datasetId.underlying,
        "secondary" -> secondary.storeId,
        "endingDataVersion" -> job.endingDataVersion
      ) {
        // This dataset _should_ not already be in the working set... if it is, this exits.
        SecondaryWatcherClaimManager.workingOn(secondary.storeId, job.datasetId)

        log.info(">> Syncing {} into {}", job.datasetId, secondary.storeId)
        if(job.replayNum > 0) log.info("Replay #{} of {}", job.replayNum, maxReplays)
        if(job.retryNum > 0) log.info("Retry #{} of {}", job.retryNum, maxRetries)
        try {
          playbackToSecondary(secondary, job)
          log.info("<< Sync done for {} into {}", job.datasetId, secondary.storeId)
        } catch {
          case bdse@BrokenDatasetSecondaryException(reason) =>
            log.error("Dataset version declared to be broken while updating dataset {} in secondary {}; marking it as broken",
              job.datasetId.asInstanceOf[AnyRef], secondary.storeId, bdse)
            manifest(u).markSecondaryDatasetBroken(job)
          case rlse@ReplayLaterSecondaryException(reason, cookie) =>
            if(job.replayNum < maxReplays) {
              val replayAfter = Math.min(replayWait.toSeconds * Math.log(job.replayNum + 2), maxReplayWait.toSeconds)
              log.info("Replay later requested while updating dataset {} in secondary {}, replaying in {}...",
                job.datasetId.asInstanceOf[AnyRef], secondary.storeId, replayAfter.toString, rlse)
              manifest(u).updateReplayInfo(secondary.storeId, job.datasetId, cookie, job.replayNum + 1,
                replayAfter.toInt)
            } else {
              log.error("Ran out of replay attempts while updating dataset {} in secondary {}; marking it as broken",
                job.datasetId.asInstanceOf[AnyRef], secondary.storeId, rlse)
              manifest(u).markSecondaryDatasetBroken(job)
            }
          case ResyncLaterSecondaryException(reason) =>
            log.info("resync later {} {} {} {}", secondary.groupName, secondary.storeId, job.datasetId.toString, reason)
            manifest(u).updateRetryInfo(job.storeId, job.datasetId, job.retryNum, backoffInterval.toSeconds.toInt)
          case e: Exception =>
            if(job.retryNum < maxRetries) {
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
          } finally {
            SecondaryWatcherClaimManager.doneWorkingOn(secondary.storeId, job.datasetId)
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
  def cleanOrphanedJobs(secondaryConfigInfo: SecondaryConfigInfo): Unit = {
    // At lean up any orphaned jobs which may have been created by this watcher
    for { u <- universe } yield {
      u.secondaryManifest.cleanOrphanedClaimedDatasets(secondaryConfigInfo.storeId, claimantId)
    }
  }

  def mainloop(secondaryConfigInfo: SecondaryConfigInfo, secondary: Secondary[CT, CV], finished: CountDownLatch): Unit = {
    var lastWrote = new DateTime(0L)
    var nextRunTime = bestNextRunTime(secondaryConfigInfo.storeId,
      secondaryConfigInfo.nextRunTime,
      secondaryConfigInfo.runIntervalSeconds)

    var done = maybeSleep(secondaryConfigInfo.storeId, nextRunTime, finished)
    while(!done) {
      try {
        for {u <- universe} yield {
          import u._

          while(run(u, new NamedSecondary(secondaryConfigInfo.storeId, secondary, secondaryConfigInfo.groupName)) &&
                finished.getCount != 0) {
            // loop until we either have no more work or we are told to exit
          }

          nextRunTime = bestNextRunTime(secondaryConfigInfo.storeId,
            nextRunTime.plus(Seconds.seconds(secondaryConfigInfo.runIntervalSeconds)),
            secondaryConfigInfo.runIntervalSeconds)

          // we only actually write at most once every 15 minutes...
          val now = DateTime.now()
          if(now.getMillis - lastWrote.getMillis >= 15 * 60 * 1000) {
            log.info("Writing new next-runtime: {}", nextRunTime)
            secondaryStoresConfig.updateNextRunTime(secondaryConfigInfo.storeId, nextRunTime)
          }
          lastWrote = now
        }

        done = maybeSleep(secondaryConfigInfo.storeId, nextRunTime, finished)
      } catch {
        case e: Exception =>
          log.error("Unexpected exception while updating claimedAt time for secondary sync jobs claimed by watcherId " +
                    claimantId.toString(), e)
      }
    }
  }
}

object SecondaryWatcherClaimManager {
  private val log = LoggerFactory.getLogger(classOf[SecondaryWatcherClaimManager])

  // in-memory list of datasets that are actively being worked on by _this_ instance
  private val workingSet = new mutable.HashSet[(String, Long)] with mutable.SynchronizedSet[(String, Long)]

  def workingOn(storeId: String, datasetId: DatasetId): Unit = {
    if (workingSet.contains((storeId, datasetId.underlying))) {
      log.error("We have already claimed dataset {} for store {} in our working set." +
        s" An unexpected error has occurred; exiting.", datasetId, storeId)
      sys.exit(1)
    }

    workingSet += ((storeId, datasetId.underlying))
    log.debug(s"Added dataset {} for store $storeId to our working set which is now {}.", datasetId, workingSet)
  }

  def doneWorkingOn(storeId: String, datasetId: DatasetId): Unit = {
    workingSet -= ((storeId, datasetId.underlying))
    log.debug(s"Removed dataset {} for store $storeId from our working set which is now {}.", datasetId, workingSet)
  }

  def andInWorkingSetSQL: String = workingSet.headOption.map { _ =>
    workingSet.map(_._2).mkString(" AND dataset_system_id IN (", ",", ")") }.getOrElse("")
}

class SecondaryWatcherClaimManager(dsInfo: DSInfo, claimantId: UUID, claimTimeout: FiniteDuration) {
  val log = LoggerFactory.getLogger(classOf[SecondaryWatcherClaimManager])

  // A claim on a dataset on the secondary manifest expires after claimTimeout.
  //
  // To maintain our claim on a dataset we will update the claimed_at timestamp every
  // updateInterval = claimTimeout / 4 < claimTimeout, therefore as we update our claims on a shorter interval
  // than that they timeout, as long as our instance is running we will not lose our claims.
  //
  // To ensure that we are actually updating the claimed_at timestamp on the interval we intend to, with another thread
  // we audit when we last updated the claimed_at timestamp every checkUpdateInterval = claimedTimeout / 2.
  // Since checkUpdateInterval < claimTimeout and > updateInterval, we will catch a failure to update our claims
  // frequently enough before we lose our claims, and then shutdown and abort work on claims that will be lost.
  //
  // This way we can protect ourselves from having multiple threads / processes mistakenly claiming the same
  // store-dataset pairs.
  val updateInterval = claimTimeout / 4
  val checkUpdateInterval = claimTimeout / 2

  // We update this value after each time we successfully update the claimed_at time.
  // A separate ScheduledExecutor thread checks that this var gets updated frequently enough.
  var lastUpdate = Long.MinValue

  // Note: the finished CountDownLatch that gets passed signals that all job processing is (should be) done

  def scheduleHealthCheck(finished: CountDownLatch): Unit = {
    SecondaryWatcherScheduledExecutor.schedule(
      checkFailed = finished.getCount() > 0 && lastUpdate + checkUpdateInterval.toMillis < System.currentTimeMillis(),
      name = "update claimed_at time",
      interval = checkUpdateInterval
    )
  }

  def mainloop(finished: CountDownLatch): Unit = {
    var done = awaitEither(finished, updateInterval.toMillis)
    while(!done) {
      def retryingUpdate(failureTimeoutMillis: Long, timeoutMillis: Long = 100): Unit = {
        if (System.currentTimeMillis() < failureTimeoutMillis) {
          try {
            updateDatasetClaimedAtTime()
            lastUpdate = System.currentTimeMillis()
            log.debug("Updated claimed_at time successfully")
          } catch {
            case e: Exception =>
              log.warn("Unexpected exception while updating claimedAt time for secondary sync jobs " +
                "claimed by watcherId " + claimantId.toString() + ". Going to retry...", e)
              // if we have been told to terminate we can just do that; otherwise retry
              if (!awaitEither(finished, timeoutMillis)) retryingUpdate(failureTimeoutMillis, 2 * timeoutMillis)
          }
        } else {
          // else: the scheduled executor will deal with this
          log.warn("Failed to update claimed_at time before timing-out.")
        }
      }
      val failureTimeoutMillis = System.currentTimeMillis() + updateInterval.toMillis
      retryingUpdate(failureTimeoutMillis) // initial timeout is .5 second with a backoff of 2 * timeout
      val remainingTimeMillis = math.max(failureTimeoutMillis - System.currentTimeMillis(), 0.toLong)

      done = awaitEither(finished, remainingTimeMillis) // await for the rest of the updateInterval
    }
  }

  private def awaitEither(finished: CountDownLatch, interval: Long): Boolean = {
    finished.await(interval, TimeUnit.MILLISECONDS)
  }

  private def updateDatasetClaimedAtTime(): Unit = {
    // TODO: We have a difficult to debug problem here if our connection pool isn't large enough to satisfy
    // all our workers, since it can block claim updates and result in claims being overwritten.
    // For now we are working around it by configuring a large pool and relying on worker config to control
    // concurrency.  We could potentially have a separate pool for the claim manager.
    using(dsInfo.dataSource.getConnection()) { conn =>
      using(conn.prepareStatement(
        s"""UPDATE secondary_manifest
           |SET claimed_at = CURRENT_TIMESTAMP
           |WHERE claimant_id = ? AND (dataset_system_id, store_id) IN (
           |  SELECT dataset_system_id, store_id FROM secondary_manifest
           |  WHERE claimant_id = ?${SecondaryWatcherClaimManager.andInWorkingSetSQL}
           |  ORDER BY dataset_system_id, store_id FOR UPDATE
           |)""".stripMargin)) { stmt =>
        stmt.setObject(1, claimantId)
        stmt.setObject(2, claimantId)
        stmt.executeUpdate()
      }
    }
  }
}

object SecondaryWatcherScheduledExecutor {
  val log = LoggerFactory.getLogger(classOf[SecondaryWatcher[_,_]])

  private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)

  def shutdown() = SecondaryWatcherScheduledExecutor.scheduler.shutdownNow()

  def schedule(checkFailed: => Boolean, name: String, interval: FiniteDuration): Unit = {
    val runnable = new Runnable() {
      def run(): Unit = {
        try {
          Thread.currentThread().setName("SecondaryWatcher scheduled health checker")
          log.debug("Running scheduled health check of: {}", name)
          if (checkFailed) {
            log.error("Failed scheduled health check of: {}; exiting.", name)
            sys.exit(1)
          }
        } catch {
          case e: Exception =>
            log.error(s"Unexpected exception while attempting to run scheduled health check for: $name; exiting.", e)
            sys.exit(1)
        }
      }
    }

    try {
      // If any execution of the task encounters an exception, subsequent executions are suppressed.
      // Hopefully the try-catch we have in run() will catch this and exit.
      scheduler.scheduleAtFixedRate(runnable, interval.toMillis, interval.toMillis, TimeUnit.MILLISECONDS)
    } catch {
      case e: Exception =>
        log.error(s"Unexpected exception while attempting to schedule health check for: $name; exiting.", e)
        sys.exit(1)
    }
  }
}

object SecondaryWatcher {
  type UniverseType[CT, CV] = Universe[CT, CV] with SecondaryManifestProvider with
    PlaybackToSecondaryProvider with
    SecondaryStoresConfigProvider
}

object SecondaryWatcherApp {
  def apply(secondaryProvider: Config => Secondary[SoQLType, SoQLValue]) {
    val rootConfig = ConfigFactory.load()
    val config = new SecondaryWatcherConfig(rootConfig, "com.socrata.coordinator.secondary-watcher")
    PropertyConfigurator.configure(Propertizer("log4j", config.log4j))

    val log = LoggerFactory.getLogger(classOf[SecondaryWatcher[_,_]])
    log.info(s"Starting secondary watcher with watcher claim uuid of ${config.watcherId}")

    val metricsOptions = MetricsOptions(config.metrics)

    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      def uncaughtException(t: Thread, e: Throwable): Unit = {
        log.error(s"Uncaught exception in thread ${t.getName}, exiting", e)
        sys.exit(1)
      }
    })

    for { dsInfo <- DataSourceFromConfig(config.database)
          reporter <- MetricsReporter.managed(metricsOptions) } {
      val secondaries = config.secondaryConfig.instances.keysIterator.map { instanceName =>
        instanceName -> secondaryProvider(config.secondaryConfig.instances(instanceName).config)
      }.toMap

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
        //Duration.fromNanos(1L),
        NullCache
      )

      val w = new SecondaryWatcher(common.universe, config.watcherId, config.claimTimeout, config.backoffInterval,
                                   config.replayWait, config.maxReplayWait, config.maxRetries,
                                   config.maxReplays.getOrElse(Integer.MAX_VALUE), common.timingReport)
      val cm = new SecondaryWatcherClaimManager(dsInfo, config.watcherId, config.claimTimeout)

      val SIGTERM = new Signal("TERM")
      val SIGINT = new Signal("INT")

      /** Flags when we want to start shutting down, don't process new work */
      val initiateShutdown = new CountDownLatch(1)
      /** Flags when we have stopped processing work and are ready to actually shutdown */
      val completeShutdown = new CountDownLatch(1)

      val signalHandler = new SignalHandler {
        val firstSignal = new java.util.concurrent.atomic.AtomicBoolean(true)
        def handle(signal: Signal): Unit = {
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

          override def run(): Unit = {
            cm.scheduleHealthCheck(completeShutdown)
            cm.mainloop(completeShutdown)
          }
        }

        val workerThreads =
          using(dsInfo.dataSource.getConnection()) { conn =>
            val cfg = new SqlSecondaryStoresConfig(conn, common.timingReport)

            secondaries.iterator.flatMap { case (name, secondary) =>
              cfg.lookup(name).map { info =>
                w.cleanOrphanedJobs(info)

                1 to config.secondaryConfig.instances(name).numWorkers map { n =>
                  new Thread {
                    setName(s"Worker $n for secondary $name")

                    override def run(): Unit = {
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

        log.info("Shutting down scheduled health checker...")
        SecondaryWatcherScheduledExecutor.shutdown()
      } finally {
        log.info("Un-hooking SIGTERM and SIGINT")
        if(oldSIGTERM != null) Signal.handle(SIGTERM, oldSIGTERM)
        if(oldSIGTERM != null) Signal.handle(SIGINT, oldSIGINT)
      }

      secondaries.values.foreach(_.shutdown())
      executor.shutdown()
    }
  }
}
