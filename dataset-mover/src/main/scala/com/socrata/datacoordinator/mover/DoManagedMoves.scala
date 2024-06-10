package com.socrata.datacoordinator.mover

import scala.util.control.Breaks

import java.io.{File, FileInputStream, InputStreamReader, BufferedReader}
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.Executors
import java.sql.DriverManager
import sun.misc.{Signal, SignalHandler}

import com.rojoma.simplearm.v2._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory

import com.socrata.http.client.HttpClientHttpClient
import com.socrata.thirdparty.typesafeconfig.Propertizer

case class DoManagedMoves(serviceConfig: MoverConfig, dryRun: Boolean, fromInstance: String, toInstance: String, trackerFile: String, systemIdListFile: String, parallelismRaw: String) {
  val parallelism = parallelismRaw.toInt

  val log = LoggerFactory.getLogger(classOf[DoManagedMoves])

  val SIGTERM = new Signal("TERM")
  val SIGINT = new Signal("INT")
  val shutdownSignalled = new AtomicBoolean(false)
  val shutdownSignalHandler = new SignalHandler {
    private val firstSignal = new AtomicBoolean(false)
    def handle(signal: Signal) {
      if (firstSignal.getAndSet(false)) {
        log.info("Signalling main thread to stop adding jobs")
        shutdownSignalled.set(true)
      } else {
        log.info("Shutdown already in progress")
      }
    }
  }

  var oldSIGTERM: SignalHandler = null
  var oldSIGINT: SignalHandler = null

  try {
    oldSIGTERM = Signal.handle(SIGTERM, shutdownSignalHandler)
    oldSIGINT = Signal.handle(SIGINT, shutdownSignalHandler)

    using(new ResourceScope) { rs =>
      implicit val executorShutdown = Resource.executorShutdownNoTimeout
      val executorService = rs.open(Executors.newCachedThreadPool)
      val httpClient = rs.open(new HttpClientHttpClient(executorService))

      val singleMover = new SingleMover(serviceConfig, dryRun, executorService, httpClient)

      val conn = DriverManager.getConnection("jdbc:sqlite:" + (if(dryRun) ":memory:" else trackerFile))
      conn.setAutoCommit(true)

      using(conn.createStatement()) { stmt =>
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS progress (system_id integer not null primary key, finished_at text null, finished_successfully boolean null)")

        val failure_reason_exists =
          using(stmt.executeQuery("select * from pragma_table_info('progress') where name = 'failure_reason'")) { rs =>
            rs.next()
          }
        if(!failure_reason_exists) {
          stmt.executeUpdate("ALTER TABLE progress ADD COLUMN failure_reason TEXT NULL");
        }
      }

      val file = locally {
        val fis = rs.open(new FileInputStream(systemIdListFile))
        val isr = rs.open(new InputStreamReader(fis, StandardCharsets.UTF_8), transitiveClose = List(fis))
        val br = rs.open(new BufferedReader(isr), transitiveClose = List(isr))
        rs.openUnmanaged(new LinesIterator(br), transitiveClose = List(br))
      }

      def finish(job: Workers.CompletedJob): Unit = {
        job match {
          case Workers.SuccessfulJob(systemId, finishedAt) =>
            using(conn.prepareStatement("UPDATE progress SET finished_at = ?, finished_successfully = ? WHERE system_id = ?")) { stmt =>
              stmt.setString(1, job.finishedAt.toString)
              stmt.setBoolean(2, true)
              stmt.setLong(3, job.systemId)
              stmt.executeUpdate()
            }
          case Workers.FailedJob(systemId, finishedAt, failureReason) =>
            using(conn.prepareStatement("UPDATE progress SET finished_at = ?, finished_successfully = ?, failure_reason = ? WHERE system_id = ?")) { stmt =>
              stmt.setString(1, job.finishedAt.toString)
              stmt.setBoolean(2, false)
              stmt.setString(3, failureReason)
              stmt.setLong(4, job.systemId)
              stmt.executeUpdate()
            }
        }
      }

      val workers = new Workers(parallelism, singleMover)

      val break = new Breaks
      break.breakable {
        for(line <- file) {
          if(shutdownSignalled.get) break.break()

          val id = line.toLong

          val inserted = using(conn.prepareStatement("INSERT INTO progress (system_id) VALUES (?) ON CONFLICT(system_id) DO NOTHING")) { stmt =>
            stmt.setLong(1, id)
            stmt.executeUpdate() != 0
          }

          if(inserted) {
            for(previousJob <- workers.submit(fromInstance, toInstance, id)) {
              finish(previousJob)
            }
          }
        }
      }

      for(job <- workers.shutdown()) {
        finish(job)
      }
    }
  } finally {
    if(oldSIGINT != null) Signal.handle(SIGINT, oldSIGINT)
    if(oldSIGTERM != null) Signal.handle(SIGTERM, oldSIGTERM)
  }
}
