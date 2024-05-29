package com.socrata.datacoordinator.secondary

import com.rojoma.simplearm.v2._
import java.io.OutputStream
import java.sql.Connection
import java.util.concurrent.{Executors, TimeUnit}
import java.util.UUID

import com.socrata.datacoordinator.secondary.messaging.NoOpMessageProducer
import org.h2.jdbcx.JdbcDataSource
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, MustMatchers}
import org.slf4j.LoggerFactory
import org.joda.time.DateTime

import scala.concurrent.duration._
import com.socrata.datacoordinator.common.SoQLCommon
import com.socrata.datacoordinator.common.collocation.NoOPCollocationLock
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.universe._
import com.socrata.datacoordinator.truth.metadata.{LifecycleStage => LS}
import com.socrata.datacoordinator.util._

class SecondaryWatcherTest extends FunSuite with MustMatchers with MockFactory {

  Class.forName("org.h2.Driver") // force driver to load

  val ds = new JdbcDataSource
  ds.setURL("jdbc:h2:mem:")

  val executor = Executors.newCachedThreadPool()
  val log = LoggerFactory.getLogger(getClass)
  val watcherId = UUID.fromString("61e9a209-98e7-4daa-9c43-5778a96e1d8a")
  val claimTimeout = (10 * 1000).millis
  val testStoreId = "testpg"
  def dummyCopyIn(c: Connection, s: String, f: OutputStream => Unit): Long = 0L

  val common = new SoQLCommon(
    ds,
    dummyCopyIn,
    executor,
    _ => None,
    new LoggedTimingReport(log) with StackedTimingReport with MetricsTimingReport with TaggableTimingReport,
    allowDdlOnPublishedCopies = false, // don't care,
    Duration.fromNanos(1L), // don't care
    "alpha-test",
    new java.io.File(System.getProperty("java.io.tmpdir")).getAbsoluteFile,
    Duration.fromNanos(1L), // don't care
    Duration.fromNanos(1L), // don't care
    //Duration.fromNanos(1L),
    NullCache
  )

  test("dataset marked broken on error when out of retries") {
    val testManifest = mock[SecondaryManifest]

    val w = new SecondaryWatcher(common.universe, watcherId, claimTimeout,
                                 10.seconds, 60.seconds, 10.minutes, 2, 10, common.timingReport,
                                 NoOpMessageProducer, NoOPCollocationLock, 100.millis) {
      override protected def manifest(u: Universe[common.CT, common.CV] with
                                         SecondaryManifestProvider with PlaybackToSecondaryProvider):
        SecondaryManifest = testManifest
    }


    for { u <- common.universe } {
      val job = SecondaryRecord(testStoreId, watcherId, new DatasetId(10),
                                startingDataVersion = 2L, endingDataVersion = 2L,
                                retryNum = 2, replayNum = 10, initialCookie = None, pendingDrop = false,
                                new DateTime(0))
      (testManifest.claimDatasetNeedingReplication _).expects(testStoreId, watcherId, claimTimeout).
                                                      returns(Some(job))

      // Mock a secondary, set up expectations
      val testSecondary = mock[Secondary[common.CT, common.CV]]
      // NOTE: these expectations are not really needed, just examples
      // (testSecondary.wantsWorkingCopies _).expects().returns(true)
      // (testSecondary.version _).expects(*, *, *, *).returns(None)

      (testManifest.markSecondaryDatasetBroken _).expects(job, job.initialCookie)

      // Run the watcher run() method
      w.run(u, new NamedSecondary(testStoreId, testSecondary, testSecondary.getClass.getName))
    }
  }

  test("dataset retry info updated when not out of retries") {
    val testManifest = mock[SecondaryManifest]

    val w = new SecondaryWatcher(common.universe, watcherId, claimTimeout,
                                 10.seconds, 60.seconds, 10.minutes, 2, 10, common.timingReport,
                                 NoOpMessageProducer, NoOPCollocationLock, 100.millis) {
      override protected def manifest(u: Universe[common.CT, common.CV] with
                                         SecondaryManifestProvider with PlaybackToSecondaryProvider):
        SecondaryManifest = testManifest
    }

    val datasetId = new DatasetId(10)

    for { u <- common.universe } {
      val job = SecondaryRecord(testStoreId, watcherId, datasetId,
                                startingDataVersion = 2L, endingDataVersion = 2L,
                                retryNum = 0, replayNum = 0, initialCookie = None, pendingDrop = false,
                                nextRetry = new DateTime(0))
      (testManifest.claimDatasetNeedingReplication _).expects(testStoreId, watcherId, claimTimeout).
                                                      returns(Some(job))

      // Mock a secondary, set up expectations
      val testSecondary = mock[Secondary[common.CT, common.CV]]

      (testManifest.updateRetryInfo _).expects(testStoreId, datasetId, 1, 10)

      // Run the watcher run() method
      w.run(u, new NamedSecondary(testStoreId, testSecondary, testSecondary.getClass.getName))
    }
  }
}
