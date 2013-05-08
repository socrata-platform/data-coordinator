package com.socrata.datacoordinator.backup

import scala.concurrent.duration._

import java.net.{SocketAddress, InetAddress, InetSocketAddress}
import java.nio.channels.spi.SelectorProvider
import java.nio.channels.{SelectionKey, SocketChannel}
import java.sql.{DriverManager, Connection}

import com.typesafe.config.ConfigFactory
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.loader.sql.{RepBasedDatasetCsvifier, SqlDelogger}
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.common.soql._
import com.socrata.datacoordinator.packets.network.{KeepaliveSetup, NetworkPackets}
import com.socrata.datacoordinator.packets.{Packet, PacketsOutputStream, Packets}
import com.socrata.datacoordinator.truth.metadata.sql._
import com.socrata.datacoordinator.id.{RowId, GlobalLogEntryId, DatasetId}
import annotation.tailrec
import com.socrata.soql.types.{SoQLValue, SoQLType, SoQLNull}
import org.xerial.snappy.SnappyOutputStream
import java.io.OutputStreamWriter
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import util.control.ControlThrowable
import scala.Some
import com.socrata.datacoordinator.common.util.ByteCountingOutputStream
import scala.Some
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.util.{NoopTimingReport, TimingReport}
import scala.Some
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.common.{DataSourceConfig, DataSourceFromConfig}
import org.apache.log4j.PropertyConfigurator
import com.socrata.thirdparty.typesafeconfig.Propertizer

final abstract class Transmitter

object Transmitter extends App {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Transmitter])

  val config = ConfigFactory.load()
  println(config.root.render)
  val backupConfig = config.getConfig("com.socrata.backup.transmitter")
  PropertyConfigurator.configure(Propertizer("log4j", backupConfig.getConfig("log4j")))

  val address = new InetSocketAddress(InetAddress.getByName(backupConfig.getString("network.backup-host")), backupConfig.getInt("network.port"))
  val maxPacketSize = backupConfig.getInt("network.max-packet-size")
  val connectTimeout = backupConfig.getMilliseconds("network.connect-timeout").longValue.milliseconds
  val newTaskAcknowledgementTimeout = backupConfig.getMilliseconds("network.new-task-acknowledgement-timeout").longValue.milliseconds
  val pollInterval = backupConfig.getMilliseconds("database.poll-interval").longValue.milliseconds

  val provider = SelectorProvider.provider

  val (dataSource, _) = DataSourceFromConfig(new DataSourceConfig(backupConfig.getConfig("database")))

  val rowCodecFactory = () => SoQLRowLogCodec
  val protocol = new Protocol(new LogDataCodec(rowCodecFactory))
  import protocol._

  val typeContext = SoQLTypeContext
  def genericRepFor(columnInfo: ColumnInfo[SoQLType]): SqlColumnRep[SoQLType, SoQLValue] =
    SoQLRep.sqlRep(columnInfo)
  def repSchema(schema: ColumnIdMap[ColumnInfo[SoQLType]]): ColumnIdMap[SqlColumnRep[SoQLType, SoQLValue]] =
    schema.mapValuesStrict(genericRepFor)
  val timingReport = NoopTimingReport

  using(provider.openSocketChannel()) { socket =>
    connect(socket, address, connectTimeout)
    KeepaliveSetup(socket)

    val client = new NetworkPackets(socket, maxPacketSize)

    while(true) {
      using(dataSource.getConnection()) { conn =>
        val playback = new PostgresGlobalLogPlayback(conn)
        val backupMfst = new PostgresBackupPlaybackManifest(conn)
        val lastJob = backupMfst.lastJobId()
        val tasks = playback.pendingJobs(lastJob).buffered
        if(tasks.nonEmpty) {
          send(client, conn, lastJob, backupMfst, tasks, timingReport)
        } else {
          client.send(NothingYet())
          client.receive() match {
            case Some(OkStillWaiting()) =>
              // good, you're still there...
              Thread.sleep(pollInterval.toMillis)
            case Some(_) =>
              ??? // TODO: unexpected packet
            case None =>
              ??? // TODO: EOF
          }
        }
      }
    }
  }

  def send(socket: Packets, conn: Connection, initialLastJobId: GlobalLogEntryId, backupMfst: PlaybackManifest, tasks: TraversableOnce[GlobalLogPlayback#Job], timingReport: TimingReport) {
    var lastJobId = initialLastJobId
    for(job <- tasks) {
      assert(job.id.underlying == lastJobId.underlying + 1, "MISSING JOBS IN GLOBAL QUEUE!!!!")
      lastJobId = job.id

      val datasetMap = new PostgresDatasetMapReader(conn, typeContext.typeNamespace, timingReport)
      log.info("Sending dataset {}'s version {}", job.datasetId.underlying, job.version)
      try {
        datasetMap.datasetInfo(job.datasetId) match {
          case Some(datasetInfo) =>
            socket.send(DatasetUpdated(job.datasetId, job.version))
            val delogger = new SqlDelogger(conn, datasetInfo.logTableName, rowCodecFactory)
            try {
              for {
                it <- managed(delogger.delog(job.version))
                event <- it
              } {
                log.info("Sending LogData({})", event)
                socket.send(LogData(event))
                socket.poll() match {
                  case Some(AlreadyHaveThat()) =>
                    log.info("Backup says it already has this version.  Abandoning the send.")
                    socket.send(DataDone())
                    backupMfst.finishedJob(job.id)
                    throw new AbortJobButDontResync
                  case Some(ResyncRequired()) =>
                    log.warn("Backup signalled that it wants a resync; abandoning logdata send")
                    throw new ResyncRequested
                  case Some(_) =>
                    log.error("Received unexpected packet from backup")
                    ??? // TODO: unexpected packet
                  case None =>
                  // ok, just keep on sending
                }
              }
              log.info("Sending DataDone")
              socket.send(DataDone())
              socket.receive() match {
                case Some(AcknowledgeReceipt()) =>
                  log.info("Backup has acknowledged receipt and committed to its store.")
                  backupMfst.finishedJob(job.id)
                case Some(AlreadyHaveThat()) =>
                  log.info("Backup says it already has this version.  Oh well, sent it unnecessarily.")
                  backupMfst.finishedJob(job.id)
                case Some(ResyncRequired()) =>
                  log.warn("Backup signalled that it wants a resync")
                  throw new ResyncRequested
                case None =>
                  ??? // TODO: EOF
              }
            } catch {
              case _: AbortJobButDontResync => // ok
            }
          case None =>
            log.warn(s"Dataset ${job.datasetId.underlying} is in the global log but there is no record of it.  It must have been deleted.")
            // TODO: Send "delete this dataset" message...
        }
      } catch {
        case _: ResyncRequested =>
          handleResyncRequest(socket, conn, job.datasetId)
          backupMfst.finishedJob(job.id)
      }
    }
  }

  class ResyncRequested extends ControlThrowable
  class AbortJobButDontResync extends ControlThrowable

  def connect(socket: SocketChannel, address: SocketAddress, timeout: FiniteDuration) {
    socket.configureBlocking(false)
    if(!socket.connect(address))  {
      using(socket.provider.openSelector()) { selector =>
        val deadline = timeout.fromNow
        val key = socket.register(selector, SelectionKey.OP_CONNECT)

        do {
          val remaining = deadline.timeLeft.toMillis
          val count =
            if(remaining <= 0) {
              if(selector.selectNow() == 0) ??? // TODO: better error
            } else {
              selector.select(remaining)
            }
        } while(!key.isConnectable || !socket.finishConnect())
      }
    }
  }

  def handleResyncRequest(client: Packets, conn: Connection, datasetId: DatasetId) {
    conn.setAutoCommit(false) // We'll be taking a lock and so we want transactions too
    val datasetMap: DatasetMapWriter[SoQLType] = new PostgresDatasetMapWriter(conn, typeContext.typeNamespace, timingReport, () => sys.error("Transmitter should never be generating obfuscation keys"), 0L)
    datasetMap.datasetInfo(datasetId, Duration.Inf) match {
      case Some(info) =>
        client.send(WillResync(info.unanchored))
        for(copy <- datasetMap.allCopies(info)) {
          awaitReadyForCopy(client)
          sendCopy(client, conn, datasetMap)(copy)
        }
        awaitReadyForCopy(client)
        client.send(NoMoreCopies())
        client.receive() match {
          case Some(ResyncComplete()) =>
            // ok good
          case Some(_) =>
            ??? // TODO: Unexpected packet
          case None =>
            ??? // TODO: EOF
        }
      case None =>
        ??? // TODO: it was just there!
    }

    conn.rollback() // release the lock and switch back to read-only mode
    conn.setAutoCommit(true)
  }

  def awaitReadyForCopy(client: Packets) {
    @tailrec
    def loop() {
      client.receive() match {
        case Some(PreparingDatabaseForResync()) =>
          loop()
        case Some(AwaitingNextCopy()) =>
          // ok good
        case Some(_) =>
          ??? // TODO: unexpected packet
        case None =>
          ??? // TODO: EOF
      }
    }
    loop()
  }

  def sendCopy(client: Packets, conn: Connection, datasetMap: DatasetMapBase[SoQLType])(copy: CopyInfo) {
    log.info("Doing full send of the copy data to the backup")
    val schema = datasetMap.schema(copy)
    val columnInfos = schema.values.map(_.unanchored).toSeq

    client.send(NextResyncCopy(copy.unanchored, columnInfos))

    if(copy.lifecycleStage != LifecycleStage.Discarded) {
      log.info("Sending CSV of the data")
      val datasetCsvifier = new RepBasedDatasetCsvifier(conn, copy.dataTableName, repSchema(schema), SoQLNull)
      // This is deliberately un-managed.  None of these streams allocate external resources,
      // so if an exception occurs, the only effect will be to not send the "end of stream"
      // packet -- which is exactly what we want to occur, so that the client doesn't believe
      // that the stream has been completed.
      val os = new PacketsOutputStream(client, dataLabel = ResyncStreamDataLabel, endLabel = ResyncStreamEndLabel)
      val postCompressedCounter = new ByteCountingOutputStream(os)
      val sos = new SnappyOutputStream(postCompressedCounter)
      val preCompressedCounter = new ByteCountingOutputStream(sos)
      val w = new OutputStreamWriter(preCompressedCounter, "UTF-8")
      datasetCsvifier.csvify(w, columnInfos.map(_.systemId))
      w.close()
      log.info("Sent {} byte(s) ({} uncompressed)", postCompressedCounter.bytesWritten, preCompressedCounter.bytesWritten)
    } else {
      log.info("Copy was discarded; not bothering to send any data")
    }
  }
}
