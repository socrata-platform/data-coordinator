package com.socrata.datacoordinator.backup

import scala.concurrent.duration._

import java.net.{SocketAddress, InetAddress, InetSocketAddress}
import java.nio.channels.spi.SelectorProvider
import java.nio.channels.{SelectionKey, SocketChannel}
import java.sql.{DriverManager, Connection}

import com.typesafe.config.ConfigFactory
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.loader.sql.SqlDelogger
import com.socrata.datacoordinator.truth.metadata.GlobalLogPlayback
import com.socrata.datacoordinator.common.soql.SoQLRowLogCodec
import com.socrata.datacoordinator.packets.network.{KeepaliveSetup, NetworkPackets}
import com.socrata.datacoordinator.packets.Packets
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresGlobalLogPlayback, PostgresDatasetMapReader}

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
      datasetMap.datasetInfo(job.datasetId) match {
        case Some(datasetInfo) =>
          socket.send(DatasetUpdated(job.datasetId, job.version))
          socket.receive(newTaskAcknowledgementTimeout) match {
            case Some(WillingToAccept()) =>
              log.info("Backup is willing to receive it")
              val delogger = new SqlDelogger(conn, datasetInfo.logTableName, rowCodecFactory)
              for {
                it <- managed(delogger.delog(job.version))
                event <- it
              } {
                log.info("Sending LogData({})", event)
                socket.send(LogData(event))
                socket.poll() match {
                  case Some(ResyncRequired()) =>
                    log.warn("Backup signalled out-of-sync!")
                    ??? // TODO: do resync AND BREAK THE ITERATION OVER "it"
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
                case Some(ResyncRequired()) =>
                  log.warn("Backup signalled out-of-sync!")
                  ??? // TODO: do resync
                case Some(AcknowledgeReceipt()) =>
                  log.info("Backup has acknowledged receipt and committed to its store.")
                  playback.finishedJob(job)
                case None =>
                  ??? // TODO: EOF
              }
            case Some(AlreadyHaveThat()) =>
              log.info("Backup says it already has this version")
              playback.finishedJob(job)
            case Some(_) =>
              log.error("Received unexpected packet from backup")
              ??? // TODO: unexpected packet
            case None =>
              ??? // TODO: EOF
          }
        case None =>
          log.warn(s"Dataset ${job.datasetId.underlying} is in the global log but there is no record of it?")
          // TODO: Send "delete this dataset" message...
      }
    }
  }

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
}
