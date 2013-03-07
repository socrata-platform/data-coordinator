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
import com.socrata.datacoordinator.common.soql.{SoQLTypeContext, SoQLRep, SoQLRowLogCodec}
import com.socrata.datacoordinator.packets.network.{KeepaliveSetup, NetworkPackets}
import com.socrata.datacoordinator.packets.{Packet, PacketsOutputStream, Packets}
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresDatasetMapWriter, PostgresGlobalLogPlayback, PostgresDatasetMapReader}
import com.socrata.datacoordinator.id.DatasetId
import annotation.tailrec
import com.socrata.soql.types.{SoQLType, SoQLNull}
import org.xerial.snappy.SnappyOutputStream
import java.io.OutputStreamWriter
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import util.control.ControlThrowable
import scala.Some
import com.socrata.datacoordinator.common.util.ByteCountingOutputStream

final abstract class Transmitter

object Transmitter extends App {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Transmitter])

  val config = ConfigFactory.load()
  println(config.root.render)
  val backupConfig = config.getConfig("com.socrata.backup.transmitter")

  val address = new InetSocketAddress(InetAddress.getByName(backupConfig.getString("network.host")), backupConfig.getInt("network.port"))
  val maxPacketSize = backupConfig.getInt("network.max-packet-size")
  val connectTimeout = backupConfig.getMilliseconds("network.connect-timeout").longValue.milliseconds
  val newTaskAcknowledgementTimeout = backupConfig.getMilliseconds("network.new-task-acknowledgement-timeout").longValue.milliseconds
  val pollInterval = backupConfig.getMilliseconds("database.poll-interval").longValue.milliseconds

  val provider = SelectorProvider.provider

  def openConnection(): Connection =
    DriverManager.getConnection(backupConfig.getString("database.url"), backupConfig.getString("database.username"), backupConfig.getString("database.password"))

  val rowCodecFactory = () => SoQLRowLogCodec
  val protocol = new Protocol(new LogDataCodec(rowCodecFactory))
  import protocol._

  val typeContext = SoQLTypeContext
  def genericRepFor(columnInfo: ColumnInfoLike): SqlColumnRep[SoQLType, Any] =
    SoQLRep.sqlRepFactories(typeContext.typeFromName(columnInfo.typeName))(columnInfo.physicalColumnBase)
  def repSchema(schema: ColumnIdMap[ColumnInfoLike]): ColumnIdMap[SqlColumnRep[SoQLType, Any]] =
    schema.mapValuesStrict(genericRepFor)

  using(provider.openSocketChannel()) { socket =>
    connect(socket, address, connectTimeout)
    KeepaliveSetup(socket)

    val client = new NetworkPackets(socket, maxPacketSize)

    while(true) {
      using(openConnection()) { conn =>
        val playback = new PostgresGlobalLogPlayback(conn)
        val tasks = playback.pendingJobs()
        if(tasks.nonEmpty) {
          send(client, conn, playback)(tasks)
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

  def send(socket: Packets, conn: Connection, playback: GlobalLogPlayback)(tasks: TraversableOnce[playback.Job]) {
    for(job <- tasks) {
      val datasetMap = new PostgresDatasetMapReader(conn)
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
                    playback.finishedJob(job)
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
                  playback.finishedJob(job)
                case Some(AlreadyHaveThat()) =>
                  log.info("Backup says it already has this version.  Oh well, sent it unnecessarily.")
                  playback.finishedJob(job)
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
          playback.finishedJob(job)
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
    val datasetMap: DatasetMapWriter = new PostgresDatasetMapWriter(conn)
    datasetMap.datasetInfo(datasetId) match {
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

  def sendCopy(client: Packets, conn: Connection, datasetMap: DatasetMapReader)(copy: CopyInfo) {
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
