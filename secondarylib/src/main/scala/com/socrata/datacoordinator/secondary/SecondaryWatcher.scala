package com.socrata.datacoordinator.secondary

import scala.collection.JavaConverters._
import com.socrata.datacoordinator.secondary.messaging.eurybates.MessageProducerConfig

import java.lang.Runnable
import java.util.UUID
import java.util.concurrent.{CountDownLatch, Executors, ScheduledExecutorService, TimeUnit, ConcurrentHashMap}

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.secondary.messaging._
import com.socrata.datacoordinator.secondary.messaging.eurybates.MessageProducerFromConfig

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Random
import com.rojoma.simplearm.v2._
import com.socrata.curator.{CuratorFromConfig}
import org.apache.curator.framework.{CuratorFramework}
import com.socrata.datacoordinator.common.{DataSourceFromConfig, SoQLCommon}
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.secondary.sql.SqlSecondaryStoresConfig
import com.socrata.datacoordinator.common.collocation.{CollocationLock, CollocationLockError, CollocationLockTimeout, CuratedCollocationLock}
import com.socrata.datacoordinator.truth.universe._
import com.socrata.datacoordinator.util._
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.thirdparty.metrics.{MetricsReporter, MetricsOptions}
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
                               timingReport: TimingReport,
                               messageProducer: MessageProducer,
                               collocationLock: CollocationLock,
                               collocationLockTimeout: FiniteDuration) {
  val log = LoggerFactory.getLogger(classOf[SecondaryWatcher[_,_]])
  private val rand = new Random()
  // splay the sleep time +/- 5s to prevent watchers from getting in lock step
  private val nextRuntimeSplay = (rand.nextInt(10000) - 5000).toLong

  // allow for overriding for easy testing
  protected def manifest(u: Universe[CT, CV] with SecondaryManifestProvider with PlaybackToSecondaryProvider):
      SecondaryManifest = u.secondaryManifest

  protected def replicationMessages(u: Universe[CT, CV] with SecondaryReplicationMessagesProvider):
      SecondaryReplicationMessages[CT, CV] = u.secondaryReplicationMessages(messageProducer)

  private[secondary] val inProgress = new SecondaryWatcher.InProgress

  def run(u: Universe[CT, CV] with Commitable with PlaybackToSecondaryProvider with SecondaryManifestProvider with SecondaryReplicationMessagesProvider with SecondaryMoveJobsProvider,
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
        val startingMillis = System.currentTimeMillis()

        // this exists so that if two threads try to replicate the
        // same dataset (which shouldn't happen) we don't want the
        // inProgress object to get confused
        val disambiguate = new Object

        try {
          inProgress.put((disambiguate, secondary.storeId, job.datasetId), ())
          playbackToSecondary(secondary, job)
          manifest(u).updateRetryInfo(job.storeId, job.datasetId, 0, 0) // done with the job, reset the retry counter
          log.info("<< Sync done for {} into {}", job.datasetId, secondary.storeId)
          completeSecondaryMoveJobs(u, job)
          // Essentially, this simulates unclaimDataset in a finally block.  That is, make sure we clean up whether
          // there is exception or not.  We don't do it the normal way (finally block) because we
          // want to trigger event message only when there is no error.
          unclaimDataset(u, secondary, job, sendMessage = true, startingMillis)
        } catch {
          case ex: Exception =>
            try {
              handlePlaybackErrors(u, secondary, job, ex)
            } finally {
              unclaimDataset(u, secondary, job, sendMessage = false, startingMillis)
            }
        } finally {
          inProgress.remove((disambiguate, secondary.storeId, job.datasetId))
        }
      }
    }
    foundWorkToDo.isDefined
  }

  private def handlePlaybackErrors(u: Universe[CT, CV] with Commitable with PlaybackToSecondaryProvider
                                                       with SecondaryManifestProvider with SecondaryReplicationMessagesProvider,
                                   secondary: NamedSecondary[CT, CV],
                                   job: SecondaryRecord,
                                   error: Exception): Unit = {
      error match {
        case bdse@BrokenDatasetSecondaryException(reason, cookie) =>
          log.error("Dataset version declared to be broken while updating dataset {} in secondary {}; marking it as broken",
            job.datasetId.asInstanceOf[AnyRef], secondary.storeId, bdse)
          // Prefer cookie handed-back by the exception, but since we write notes in out cookies currently that we don't
          // want to lose, let's avoid overriding those if we are not given Some other cookie string
          manifest(u).markSecondaryDatasetBroken(job, cookie.orElse(job.initialCookie))
        case rlse@ReplayLaterSecondaryException(reason, cookie) =>
          if (job.replayNum < maxReplays) {
            val replayAfter = Math.min(replayWait.toSeconds * Math.pow(2, job.replayNum), maxReplayWait.toSeconds)
            log.info("Replay later requested while updating dataset {} in secondary {}, replaying in {}...",
              job.datasetId.asInstanceOf[AnyRef], secondary.storeId, replayAfter.toString, rlse)
            manifest(u).updateReplayInfo(secondary.storeId, job.datasetId, cookie, job.replayNum + 1,
              replayAfter.toInt)
          } else {
            log.error("Ran out of replay attempts while updating dataset {} in secondary {}; marking it as broken",
              job.datasetId.asInstanceOf[AnyRef], secondary.storeId, rlse)
            manifest(u).markSecondaryDatasetBroken(job, job.initialCookie)
          }
        case ResyncLaterSecondaryException(reason) =>
          log.info("resync later {} {} {} {}", secondary.groupName, secondary.storeId, job.datasetId.toString, reason)
          manifest(u).updateRetryInfo(job.storeId, job.datasetId, job.retryNum, backoffInterval.toSeconds.toInt)
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
            manifest(u).markSecondaryDatasetBroken(job, job.initialCookie)
          }
      }
  }


  private def unclaimDataset(u: Universe[CT, CV] with Commitable with PlaybackToSecondaryProvider
                                                 with SecondaryManifestProvider with SecondaryReplicationMessagesProvider,
                             secondary: NamedSecondary[CT, CV],
                             job: SecondaryRecord, sendMessage: Boolean, startingMillis: Long): Unit ={
    try {
      // We need to remove the job from our in memory list before we unclaim it or we have a race condition where
      // another thread claims it and errors out because it thinks we are still working on it.  The main goal of
      // tracking what jobs we are working on in memory is to avoid continuing to update our claim on a job
      // that has experienced an unexpected error.  A secondary purpose is to catch logic errors if we were to end up
      // claiming a job twice.  Both of those goals are compatible with first removing the job from our in memory
      // list and then unclaiming it.
      SecondaryWatcherClaimManager.doneWorkingOn(secondary.storeId, job.datasetId)

      manifest(u).releaseClaimedDataset(job)
      u.commit()

      log.info("finished version: {}", job.endingDataVersion)

      // logic for sending messages to amq
      if (sendMessage) {
        try {
          replicationMessages(u).send(
            datasetId = job.datasetId,
            storeId = job.storeId,
            endingDataVersion = job.endingDataVersion,
            startingMillis = startingMillis,
            endingMillis = System.currentTimeMillis()
          )
        } catch {
          case e: DatasetCopyLessThanDataVersionNotFound =>
            log.warn(s"Metrics send message: ${e.getMessage} Continuing regardless...")
          case e: Exception =>
            log.error("Unexpected exception sending message! Continuing regardless...", e)
        }
      }
    } catch {
      case e: Exception =>
        log.error("Unexpected exception while releasing claim on dataset {} in secondary {}",
          job.datasetId.asInstanceOf[AnyRef], secondary.storeId, e)
    }
  }

  private def completeSecondaryMoveJobs(u: Universe[CT, CV] with SecondaryManifestProvider
                                                            with PlaybackToSecondaryProvider
                                                            with SecondaryMoveJobsProvider,
                                        job: SecondaryRecord): Unit = {
    val moveJobs = u.secondaryMoveJobs

    val storeId = job.storeId
    val datasetId = job.datasetId

    def forLog(jobs: Seq[SecondaryMoveJob]): String = jobs.map(_.id).toString()

    if (job.pendingDrop) {
      // For pending drops when "completing" move jobs we acquire a collocation lock to avoid race conditions
      // Note: the only currently automated process to mark a dataset for pending drop is here in the secondary
      // watcher after a dataset has completed replication to a destination store in a move job.
      try {
        log.info("Attempting to acquire collocation lock for pending drop of dataset.")
        if (collocationLock.acquire(collocationLockTimeout.toMillis)) {
          try {
            log.info("Acquired collocation lock for pending drop of dataset.")

            val movesToStore = moveJobs.jobsToStore(storeId, datasetId)

            if (movesToStore.nonEmpty) {
              log.error("Upon pending drop of dataset {} from store {} there are unexepected jobs moving to the store: {}",
                datasetId.toString, storeId.toString, forLog(movesToStore))
              throw new Exception("Unexpected jobs moving dataset to store when it is pending drop!")
            }

            val movesFromStore = moveJobs.jobsFromStore(storeId, datasetId)
            log.info("Upon pending drop of dataset {} from store {} completed moves for jobs: {}",
              datasetId.toString, storeId.toString, forLog(movesFromStore))
            moveJobs.markJobsFromStoreComplete(storeId, datasetId)
          } finally {
            collocationLock.release()
          }
        } else {
          log.error("Failed to acquire collocation lock during pending drop of {}", datasetId)
          throw CollocationLockTimeout(collocationLockTimeout.toMillis)
        }
      } catch {
        case error: CollocationLockError =>
          log.error("Unexpected error with collocation lock during pending drop of dataset!", error)
          throw error
      }
    } else {
      val movesToStore = moveJobs.jobsToStore(storeId, datasetId)

      log.info("Upon replicating dataset {} to store {} started moves for jobs: {}",
        datasetId.toString, storeId.toString, forLog(movesToStore))
      moveJobs.markJobsToStoreComplete(storeId, datasetId)

      // initiate drop from secondary stores
      movesToStore.foreach { move =>
        manifest(u).markDatasetForDrop(move.fromStoreId, job.datasetId)
      }
    }
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
    // At startup clean up any orphaned jobs which may have been created by this watcher
    for { u <- universe } {
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
        for {u <- universe} {
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
          // avoid tight spin loop if we have recurring errors, eg. unable to talk to db
          Thread.sleep(10L * 1000)
      }
    }
  }
}

object SecondaryWatcherClaimManager {
  private val log = LoggerFactory.getLogger(classOf[SecondaryWatcherClaimManager])

  // in-memory list of datasets that are actively being worked on by _this_ instance
  private object X // like Unit, but nullable because Java
  private val workingSet = new ConcurrentHashMap[(String, Long), X.type]

  def workingOn(storeId: String, datasetId: DatasetId): Unit = {
    if(workingSet.putIfAbsent((storeId, datasetId.underlying), X) ne null) {
      log.error("We have already claimed dataset {} for store {} in our working set." +
        s" An unexpected error has occurred; exiting.", datasetId, storeId)
      sys.exit(1)
    }

    log.debug(s"Added dataset {} for store $storeId to our working set which is now {}.", datasetId, workingSet)
  }

  def doneWorkingOn(storeId: String, datasetId: DatasetId): Unit = {
    workingSet.remove((storeId, datasetId.underlying))
    log.debug(s"Removed dataset {} for store $storeId from our working set which is now {}.", datasetId, workingSet)
  }

  def andInWorkingSetSQL: String = {
    val ids = workingSet.keySet.iterator.asScala.map(_._2).to[Vector]
    if(ids.nonEmpty) {
      ids.mkString(" AND dataset_system_id IN (", ",", ")")
    } else {
      ""
    }
  }
}

class SecondaryWatcherClaimManager(dsInfo: DSInfo, claimantId: UUID, claimTimeout: FiniteDuration, inProgress: SecondaryWatcher.InProgress) {
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
                 |WHERE claimant_id = ? AND store_id = ? AND dataset_system_id = ?
                 |""".stripMargin)) { stmt =>
        var didOne = false;
        for((_, storeId, datasetId) <- inProgress.keys.asScala) {
          didOne = true
          stmt.setObject(1, claimantId)
          stmt.setString(2, storeId)
          stmt.setLong(3, datasetId.underlying)
          stmt.addBatch()
        }
        if(didOne) stmt.executeBatch()
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
            log.error("Failed scheduled health check of: {}; exiting.  Increase claim-timeout in config for break/step when debugging.", name)
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
  type UniverseType[CT, CV] = Universe[CT, CV] with Commitable with
                                                    PlaybackToSecondaryProvider with
                                                    SecondaryManifestProvider with
                                                    SecondaryMoveJobsProvider with
                                                    SecondaryReplicationMessagesProvider with
                                                    SecondaryStoresConfigProvider

  type InProgress = ConcurrentHashMap[(Object, String, DatasetId), Unit]
}

object SecondaryWatcherApp {
  type NumWorkers = Int
  def apply
    (dsInfo: Managed[DSInfo], reporter: Managed[MetricsReporter], curator: Managed[CuratorFramework])
    (secondaryConfig: SecondaryConfig)
    (secondaryWatcherConfig: SecondaryWatcherAppConfig)
    (secondaries: Map[String, (Secondary[SoQLType, SoQLValue], NumWorkers)]
    ): Unit =  {
    val log = LoggerFactory.getLogger(classOf[SecondaryWatcher[_,_]])
    log.info(s"Starting secondary watcher with watcher claim uuid of ${secondaryWatcherConfig.watcherId}")

    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      def uncaughtException(t: Thread, e: Throwable): Unit = {
        log.error(s"Uncaught exception in thread ${t.getName}, exiting", e)
        sys.exit(1)
      }
    })

    for { dsInfo <- dsInfo
      reporter <- reporter
      curator <- curator
    } {
      val executor = Executors.newCachedThreadPool()

      val collocationLock = new CuratedCollocationLock(curator, secondaryWatcherConfig.collocationLockPath)

      val common = new SoQLCommon(
        dsInfo.dataSource,
        dsInfo.copyIn,
        executor,
        _ => None,
        new LoggedTimingReport(log) with StackedTimingReport with MetricsTimingReport with TaggableTimingReport,
        allowDdlOnPublishedCopies = false, // don't care,
        Duration.fromNanos(1L), // don't care
        secondaryWatcherConfig.instance,
        secondaryWatcherConfig.tmpdir,
        Duration.fromNanos(1L), // don't care
        Duration.fromNanos(1L), // don't care
                                //Duration.fromNanos(1L),
        NullCache
      )
      val messageProducerExecutor = Executors.newCachedThreadPool()
      val messageProducer = MessageProducerFromConfig(secondaryWatcherConfig.watcherId, messageProducerExecutor, secondaryConfig.messageProducerConfig)
      messageProducer.start()

      val w = new SecondaryWatcher(common.universe, secondaryWatcherConfig.watcherId, secondaryConfig.claimTimeout, secondaryConfig.backoffInterval,
        secondaryConfig.replayWait, secondaryConfig.maxReplayWait, secondaryConfig.maxRetries,
        secondaryConfig.maxReplays.getOrElse(Integer.MAX_VALUE), common.timingReport,
        messageProducer, collocationLock, secondaryConfig.collocationLockTimeout)
      val cm = new SecondaryWatcherClaimManager(dsInfo, secondaryWatcherConfig.watcherId, secondaryConfig.claimTimeout, w.inProgress)

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

        val inProgress = new SecondaryWatcher.InProgress
        val workerThreads =
          using(dsInfo.dataSource.getConnection()) { conn =>
            val cfg = new SqlSecondaryStoresConfig(conn, common.timingReport)

            secondaries.iterator.flatMap { case (name, (secondary, numWorkers)) =>
              cfg.lookup(name).map { info =>
                w.cleanOrphanedJobs(info)

                1 to numWorkers map { n =>
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
        log.info("Shutting down message producer...")
        messageProducer.shutdown()
        messageProducerExecutor.shutdown()

        log.info("Un-hooking SIGTERM and SIGINT")
        if(oldSIGTERM != null) Signal.handle(SIGTERM, oldSIGTERM)
        if(oldSIGTERM != null) Signal.handle(SIGINT, oldSIGINT)
      }

      secondaries.values.foreach { case (secondary, _) => secondary.shutdown()}
      executor.shutdown()
    }
  }


  def apply(secondaryProvider: Config => Secondary[SoQLType, SoQLValue]): Unit = {
    val rootConfig = ConfigFactory.load()
    val config = new SecondaryWatcherConfig(rootConfig, "com.socrata.coordinator.secondary-watcher")


    val secondaries: Map[String, (Secondary[SoQLType, SoQLValue], NumWorkers)] =
      config.secondaryConfig.instances.keysIterator.map { instanceName =>
        instanceName -> {
          val cfg = config.secondaryConfig.instances(instanceName)
          secondaryProvider(cfg.config) -> cfg.numWorkers
        }
      }.toMap

    apply(
      DataSourceFromConfig(config.database),
      MetricsReporter.managed(config.metrics),
      CuratorFromConfig(config.curator)
    )(config)(config)(secondaries)
  }
}
