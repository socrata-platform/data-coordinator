package com.socrata.datacoordinator.backup

import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import java.net._
import java.nio.channels.spi.SelectorProvider
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.packets.network.NetworkPackets
import com.socrata.datacoordinator.packets.{Packets, ProtocolError, Packet, PacketOutputStream}
import com.socrata.datacoordinator.truth.metadata.DatasetInfo
import scala.annotation.tailrec
import com.socrata.datacoordinator.truth.loader.Delogger
import java.sql.{DriverManager, Connection}
import java.nio.ByteBuffer
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.loader.sql.SqlLogger
import com.socrata.datacoordinator.common.util.ByteBufferInputStream
import java.io.{OutputStreamWriter, InputStream}
import com.socrata.datacoordinator.common.soql.SoQLRowLogCodec
import com.socrata.datacoordinator.truth.sql.DatabasePopulator

final abstract class Receiver

object Receiver extends App {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Receiver])
  val config = ConfigFactory.load()
  println(config.root.render)
  val receiverConfig = config.getConfig("com.socrata.backup.receiver")

  val address = new InetSocketAddress(InetAddress.getByName(receiverConfig.getString("network.host")), receiverConfig.getInt("network.port"))
  val reuseAddr = receiverConfig.getBoolean("network.reuse-address")
  val idleTimeout = receiverConfig.getMilliseconds("network.idle-timeout").longValue.milliseconds
  val dataTimeout = receiverConfig.getMilliseconds("network.data-timeout").longValue.milliseconds
  val maxPacketSize = receiverConfig.getInt("network.max-packet-size")

  val executor = java.util.concurrent.Executors.newCachedThreadPool()
  val provider = SelectorProvider.provider

  val protocol = new Protocol(() => SoQLRowLogCodec)
  import protocol._

  using(openConnection()) { conn =>
    conn.setAutoCommit(false)
    DatabasePopulator.populate(conn)
    conn.commit()
  }

  using(provider.openServerSocketChannel()) { listenSocket =>
    listenSocket.setOption[java.lang.Boolean](StandardSocketOptions.SO_REUSEADDR, reuseAddr)
    listenSocket.bind(address)

    for {
      rawClient <- managed(listenSocket.accept())
      client <- managed(new NetworkPackets(rawClient, maxPacketSize))
    } {
      listenSocket.close()

      def loop() {
        client.receive(idleTimeout) match {
          case Some(NothingYet()) =>
            client.send(OkStillWaiting())
            loop()
          case Some(DatasetUpdated(id, version)) =>
            datasetUpdateRequested(id, version, client)
            loop()
          case Some(_) =>
            ??? // TODO: Unexpected packet received
          case None =>
            log.info("Other end hung up; closing")
        }
      }
      loop()
    }
  }

  sealed abstract class AcceptResult
  case object Ok extends AcceptResult
  case object Resyncing extends AcceptResult

  def openConnection(): Connection =
    DriverManager.getConnection(receiverConfig.getString("database.url"), receiverConfig.getString("database.username"), receiverConfig.getString("database.password"))

  def datasetUpdateRequested(datasetId: DatasetId, version: Long, client: Packets) {
    using(openConnection()) { conn =>
      conn.setAutoCommit(false)
      val backup = new Backup(conn, executor, paranoid = true)

      backup.datasetMap.datasetInfo(datasetId) match {
        case Some(datasetInfo) =>
          val initialVersion = backup.datasetMap.latest(datasetInfo)
          if(initialVersion.dataVersion >= version) {
            client.send(AlreadyHaveThat())
          } else if(initialVersion.dataVersion < version - 1) {
            client.send(ResyncRequired())
            waitForResyncAck(client)
            resync(datasetId, client)
          } else {
            client.send(WillingToAccept())
            acceptItAll(conn, backup)(initialVersion, client, version) match {
              case Ok =>
              // great
              case Resyncing =>
                waitForResyncAck(client)
                resync(datasetId, client)
            }
          }
        case None =>
          if(version == 1) {
            client.send(WillingToAccept())
            client.receive() match {
              case Some(LogData(Delogger.WorkingCopyCreated(ci))) =>
                val initialCopy = backup.createDataset(ci)
                acceptItAll(conn, backup)(initialCopy, client, version) match {
                  case Ok =>
                    // great
                  case Resyncing =>
                    waitForResyncAck(client)
                    resync(datasetId, client)
                }
              case Some(_) =>
                ??? // TODO: first message wasn't "working copy created?"
              case None =>
                ??? // TODO: EOF
            }
          } else {
            ??? // TODO: can't find the dataset, and we're not creating it?
          }
      }
    }
  }

  def resync(datasetId: DatasetId, client: Packets) {
    ??? // TODO
  }

  def waitForResyncAck(client: Packets) {
    def loop() {
      client.receive(dataTimeout) match {
        case Some(LogData(_)) => loop()
        case Some(DataDone()) => loop()
        case Some(WillResync()) => // done
        case None =>
      }
    }
    loop()
  }

  def acceptItAll(conn: Connection, backup: Backup)(initialCopyInfo: backup.datasetMap.CopyInfo, client: Packets, version: Long): AcceptResult = {
    @tailrec
    def loop(copy: backup.datasetMap.CopyInfo): backup.datasetMap.CopyInfo = {
      client.receive(dataTimeout) match {
        case Some(LogData(d)) =>
          log.info("Processing " + d)
          loop(backup.dispatch(copy, d))
        case Some(DataDone()) =>
          copy
        case None =>
          log.warn("Unexpected EOF")
          ??? // TODO
      }
    }

    try {
      backup.updateVersion(loop(initialCopyInfo), version)
      conn.commit()
      client.send(AcknowledgeReceipt())
      Ok
    } catch {
      case e: /* Resync */Exception =>
        conn.rollback()
        client.send(ResyncRequired())
        Resyncing
    }
  }
}

