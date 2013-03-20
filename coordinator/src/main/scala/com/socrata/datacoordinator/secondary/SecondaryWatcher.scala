package com.socrata.datacoordinator.secondary

import scala.concurrent.duration._
import com.rojoma.simplearm.{SimpleArm, Managed}

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresSecondaryPlaybackManifest, PostgresDatasetMapReader, PostgresGlobalLogPlayback}
import com.socrata.datacoordinator.secondary.sql.{SqlSecondaryConfig, SqlSecondaryManifest}
import com.socrata.datacoordinator.truth.sql.{SqlColumnRep, SqlColumnReadRep}
import com.socrata.datacoordinator.truth.metadata.{PlaybackManifest, DatasetMapReader, GlobalLogPlayback, ColumnInfo}
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
import com.socrata.datacoordinator.util.{StackedTimingReport, LoggedTimingReport, TimingReport}
import com.socrata.datacoordinator.truth.universe._
import com.socrata.datacoordinator.truth.loader.Delogger
import java.sql.Connection

class SecondaryWatcher[CT, CV](universe: => Managed[Universe[CT, CV] with DatasetMapReaderProvider with GlobalLogPlaybackProvider with SecondaryManifestProvider with SecondaryPlaybackManifestProvider with PlaybackToSecondaryProvider with DeloggerProvider with SecondaryConfigProvider]) {
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
            pb(job.datasetId, secondary, dsmr, delogger(datasetInfo.logTableName))
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
    if(remainingTime <= 0) log.warn("{} is behind schedule {}ms", storeId, remainingTime.abs)
    finished.await(remainingTime, TimeUnit.MILLISECONDS)
  }

  def mainloop(secondaryConfigInfo: SecondaryConfigInfo, secondary: Secondary[CV], finished: CountDownLatch) {
    var lastWrote = new DateTime(0L)
    var nextRunTime = secondaryConfigInfo.nextRunTime
    if(nextRunTime.compareTo(DateTime.now()) < 0) nextRunTime = DateTime.now()

    var done = maybeSleep(secondaryConfigInfo.storeId, nextRunTime, finished)
    while(!done) {
      for { u <- universe } yield {
        import u._

        run(u, new NamedSecondary(secondaryConfigInfo.storeId, secondary))

        nextRunTime = nextRunTime.plus(Seconds.seconds(secondaryConfigInfo.runIntervalSeconds))

        // we only actually write at most once every 15 minutes...
        val now = DateTime.now()
        if(now.getMillis - lastWrote.getMillis >= 15 * 60 * 1000) {
          log.info("Writing new next-runtime")
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

  val timingReport = new LoggedTimingReport(log) with StackedTimingReport

  val rootConfig = ConfigFactory.load()
  val config = rootConfig.getConfig("com.socrata.secondary-watcher")
  println(config.root.render())
  val (dataSource, _) = DataSourceFromConfig(config)
  val secondaries = SecondaryLoader.load(config.getConfig("secondary.configs"), new File(config.getString("secondary.path"))).asInstanceOf[Map[String, Secondary[Any]]]

  val pause = config.getMilliseconds("sleep-time").longValue.millis

  trait TypeInfo { this: TypeUniverse =>
    def repFor(ci: ColumnInfo): SqlColumnRep[CT, CV]
    def newRowCodec(): RowLogCodec[CV]
  }

  abstract class LocalUniverse[ColumnType, ColumnValue](conn: Connection) extends Universe[ColumnType, ColumnValue] with TypeInfo with DatasetMapReaderProvider with GlobalLogPlaybackProvider with SecondaryManifestProvider with SecondaryPlaybackManifestProvider with PlaybackToSecondaryProvider with DeloggerProvider with SecondaryConfigProvider {
    def commit() { conn.commit() }

    def secondaryPlaybackManifest(storeId: String): PlaybackManifest =
      new PostgresSecondaryPlaybackManifest(conn, storeId)

    lazy val playbackToSecondary: PlaybackToSecondary[CT, CV] =
      new PlaybackToSecondary(conn, secondaryManifest, repFor, timingReport)

    def delogger(logTableName: String): Delogger[CV] =
      new SqlDelogger(conn, logTableName, newRowCodec _)

    lazy val secondaryManifest: SecondaryManifest =
      new SqlSecondaryManifest(conn)

    lazy val datasetMapReader: DatasetMapReader =
      new PostgresDatasetMapReader(conn)

    lazy val globalLogPlayback: GlobalLogPlayback =
      new PostgresGlobalLogPlayback(conn)

    lazy val timingReport: TimingReport =
      self.timingReport

    lazy val secondaryConfig =
      new SqlSecondaryConfig(conn)
  }

  class SoQLUniverse(conn: Connection) extends LocalUniverse[SoQLType, Any](conn) with TypeInfo {
    def repFor(ci: ColumnInfo) =
      SoQLRep.sqlRepFactories(SoQLType.typesByName(TypeName(ci.typeName)))(ci.physicalColumnBaseBase)
    def newRowCodec() = SoQLRowLogCodec
  }

  val universe = new SimpleArm[SoQLUniverse] {
    def flatMap[B](f: SoQLUniverse => B): B = {
      using(dataSource.getConnection()) { conn =>
        conn.setAutoCommit(false)
        val result = f(new SoQLUniverse(conn))
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
