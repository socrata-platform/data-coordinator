package com.socrata.datacoordinator.backup

import scala.annotation.tailrec
import scala.concurrent.duration._

import java.net._
import java.nio.channels.spi.SelectorProvider
import java.sql.{DriverManager, Connection}

import com.typesafe.config.{Config, ConfigFactory}
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.packets.network.NetworkPackets
import com.socrata.datacoordinator.packets.{PacketsInputStream, Packets}
import com.socrata.datacoordinator.truth.loader.{SchemaLoader, Delogger}
import com.socrata.datacoordinator.id.{ColumnId, DatasetId}
import com.socrata.datacoordinator.common.soql.{SoQLTypeContext, SoQLRowLogCodec}
import com.socrata.datacoordinator.truth.sql.DatabasePopulator
import com.socrata.datacoordinator.common.{DataSourceConfig, DataSourceFromConfig, StandardDatasetMapLimits}
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.util.collection.{MutableColumnIdMap, ColumnIdMap}
import org.xerial.snappy.SnappyInputStream
import java.io.InputStreamReader
import util.control.ControlThrowable
import scala.Some
import com.socrata.datacoordinator.truth.metadata.DatasetInfo
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.util.NoopTimingReport
import com.socrata.soql.types.SoQLType
import org.apache.log4j.PropertyConfigurator
import com.socrata.thirdparty.typesafeconfig.Propertizer

final abstract class Receiver

class ReceiverConfig(config: Config, root: String) {
  private def k(s: String) = root + "." + s
  val log4j = config.getConfig(k("log4j"))
  val reuseAddr = config.getBoolean(k("network.reuse-address"))
  val idleTimeout = config.getMilliseconds(k("network.idle-timeout")).longValue.milliseconds
  val dataTimeout = config.getMilliseconds(k("network.data-timeout")).longValue.milliseconds
  val maxPacketSize = config.getInt(k("network.max-packet-size"))
  val host = config.getString(k("network.host"))
  val port = config.getInt(k("network.port"))
  val database = new DataSourceConfig(config, k("database"))
}

object Receiver extends App {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Receiver])
  val config = ConfigFactory.load()
  println(config.root.render)
  val receiverConfig = new ReceiverConfig(config, "com.socrata.backup.receiver")
  PropertyConfigurator.configure(Propertizer("log4j", receiverConfig.log4j))
  import receiverConfig.{reuseAddr, idleTimeout, dataTimeout, maxPacketSize}

  val address = new InetSocketAddress(InetAddress.getByName(receiverConfig.host), receiverConfig.port)

  val executor = java.util.concurrent.Executors.newCachedThreadPool()
  val provider = SelectorProvider.provider

  val datasetMapLimits = StandardDatasetMapLimits
  val timingReport = NoopTimingReport

  val typeNamespace = SoQLTypeContext.typeNamespace
  val codec = new LogDataCodec(() => SoQLRowLogCodec)
  val protocol = new Protocol(codec)
  import protocol._

  val (dataSource, copyIn) = DataSourceFromConfig(receiverConfig.database)

  using(dataSource.getConnection()) { conn =>
    conn.setAutoCommit(false)
    DatabasePopulator.populate(conn, datasetMapLimits)
    conn.commit()
  }

  using(provider.openServerSocketChannel()) { listenSocket =>
    listenSocket.setOption[java.lang.Boolean](StandardSocketOptions.SO_REUSEADDR, reuseAddr)
    listenSocket.bind(address)

    log.info("Listening for connection...")
    for {
      rawClient <- managed(listenSocket.accept())
      client <- managed(new NetworkPackets(rawClient, maxPacketSize))
    } {
      listenSocket.close()

      log.info("Received connection from {}", rawClient.getRemoteAddress)

      def loop() {
        client.receive(idleTimeout) match {
          case Some(NothingYet()) =>
            client.send(OkStillWaiting())
            loop()
          case Some(DatasetUpdated(id, version)) =>
            log.info("Dataset {} updated to version {}", id.underlying, version)
            datasetUpdateRequested(id, version, client)
            loop()
          case Some(ForceResync(datasetId)) =>
            forcedResyncRequested(datasetId, client)
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

  def datasetUpdateRequested(datasetId: DatasetId, version: Long, client: Packets) {
    using(dataSource.getConnection()) { conn =>
      conn.setAutoCommit(false)
      val backup = new Backup(conn, executor, timingReport, paranoid = true, copyIn = copyIn)

      try {
        backup.datasetMap.datasetInfo(datasetId, Duration.Inf) match {
          case Some(datasetInfo) =>
            receiveUpdate(conn, backup, client)(datasetInfo, version)
          case None =>
            log.info("I do not have dataset {}", datasetId.underlying)
            if(version == 1) {
              receiveCreate(conn, backup, client)(datasetId, version)
            } else {
              log.info("Can't find the dataset, and it is not version 1 we just received.  Resyncing.")
              throw new AbortToResync
            }
        }
      } catch {
        case _: AbortToResync =>
          conn.rollback()
          resync(conn, backup, datasetId, client)
      } finally {
        conn.rollback() // If we got a full response, we committed it earlier.  This SHOULD be a no-op.  No way to actually tell though.
      }
    }
  }

  def forcedResyncRequested(datasetId: DatasetId, client: Packets) {
    using(dataSource.getConnection()) { conn =>
      conn.setAutoCommit(false)
      val backup = new Backup(conn, executor, timingReport, paranoid = true, copyIn = copyIn)
      try {
        resync(conn, backup, datasetId, client)
      } finally {
        conn.rollback()
      }
    }
  }

  class AbortToResync extends ControlThrowable

  def receiveCreate(conn: Connection, backup: Backup, client: Packets)(datasetId: DatasetId, version: Long) {
    log.info("New dataset.  Accepting data from the primary.")
    client.receive() match {
      case Some(LogData(Delogger.WorkingCopyCreated(di, ci))) =>
        log.info("Creating initial copy")
        val initialCopy = backup.createDataset(di, ci)
        acceptItAll(conn, backup)(initialCopy, client, version)
      case Some(LogData(_)) | Some(DataDone()) =>
        log.warn("Got version 1 of dataset {} but the first event was not WorkingCopyCreated?  Resyncing!", datasetId.underlying)
        throw new AbortToResync
      case Some(_) =>
        ??? // TODO: unexpected packet
      case None =>
        ??? // TODO: EOF
    }
  }

  def receiveUpdate(conn: Connection, backup: Backup, client: Packets)(datasetInfo: DatasetInfo, version: Long) {
    val initialVersion = backup.datasetMap.latest(datasetInfo)
    log.info("I have version {} of the dataset {}", initialVersion.dataVersion, datasetInfo.systemId.underlying)
    if(initialVersion.dataVersion >= version) {
      log.info("Telling primary that I already have that version and waiting for it to say it's done.")
      client.send(AlreadyHaveThat())
      awaitDataDone(client)
    } else if(initialVersion.dataVersion < version - 1) {
      log.info("I am farther behind than that.  Resyncing.")
      throw new AbortToResync
    } else {
      log.info("I have the previous version.  Accepting data from the primary.")
      acceptItAll(conn, backup)(initialVersion, client, version)
    }
  }

  def awaitDataDone(client: Packets) {
    @tailrec
    def loop() {
      client.receive(dataTimeout) match {
        case Some(LogData(_)) => // ignore
          loop()
        case Some(DataDone()) =>
          log.info("Got data done")
        case Some(_) =>
          ??? // TODO: Unexpected packet
        case None =>
          ??? // TODO: EOF
      }
    }
    loop()
  }

  def acceptItAll(conn: Connection, backup: Backup)(initialCopyInfo: CopyInfo, client: Packets, version: Long) {
    try {
      @tailrec
      def loop(copy: CopyInfo): CopyInfo = {
        client.receive(dataTimeout) match {
          case Some(LogData(d)) =>
            log.debug("Processing a record of type {}", d.productPrefix)
            loop(backup.dispatch(copy, d))
          case Some(DataDone()) =>
            log.info("Version {} completed", version)
            copy
          case Some(_) =>
            ??? // TODO: Unexpected packet
          case None =>
            ??? // TODO: EOF
        }
      }

      val finalCopyInfo = loop(initialCopyInfo)
      backup.updateVersion(finalCopyInfo, version)
    } catch {
      case e: /* Resync */Exception =>
        log.error("Caught exception; resyncing", e)
        throw new AbortToResync
    }

    conn.commit()
    log.info("Committed; informing the primary", version)
    client.send(AcknowledgeReceipt())
  }

  def resync(conn: Connection, backup: Backup, datasetId: DatasetId, client: Packets) {
    client.send(ResyncRequired())
    val datasetInfo = waitForResyncAck(client)
    assert(datasetInfo.systemId == datasetId, "Dataset info received in response to resync request was not the same dataset")
    receiveResync(conn, backup, client, datasetInfo)
  }

  def waitForResyncAck(client: Packets): UnanchoredDatasetInfo = {
    def loop(): UnanchoredDatasetInfo = {
      client.receive(dataTimeout) match {
        case Some(WillResync(datasetInfo)) => datasetInfo
        case Some(LogData(_)) | Some(DataDone()) => loop()
        case Some(_) => ??? // TODO: unexpected packet
        case None => ??? // TODO: EOF
      }
    }
    loop()
  }

  def receiveResync(conn: Connection, backup: Backup, client: Packets, datasetInfo: UnanchoredDatasetInfo) {
    val clearedDatasetInfo = backup.datasetMap.datasetInfo(datasetInfo.systemId, Duration.Inf) match {
      case Some(originalDatasetInfo) =>
        using(conn.createStatement()) { stmt =>
          for(copy <- backup.datasetMap.allCopies(originalDatasetInfo)) {
            client.send(PreparingDatabaseForResync())
            stmt.execute("DROP TABLE IF EXISTS " + copy.dataTableName)
          }
          client.send(PreparingDatabaseForResync())
          stmt.execute("DROP TABLE IF EXISTS " + datasetInfo.logTableName)
        }
        backup.datasetMap.unsafeReloadDataset(originalDatasetInfo, datasetInfo.nextCounterValue, datasetInfo.localeName, datasetInfo.obfuscationKey)
      case None =>
        backup.datasetMap.unsafeCreateDataset(datasetInfo.systemId, datasetInfo.nextCounterValue, datasetInfo.localeName, datasetInfo.obfuscationKey)
    }

    @tailrec
    def loop() {
      client.send(AwaitingNextCopy())
      client.receive() match {
        case Some(NextResyncCopy(copyInfo, columns)) =>
          val datasetMap = backup.datasetMap
          val copy = datasetMap.unsafeCreateCopy(clearedDatasetInfo, copyInfo.systemId, copyInfo.copyNumber, copyInfo.lifecycleStage, copyInfo.dataVersion)
          val actuallyDoDataOps = copy.lifecycleStage != LifecycleStage.Discarded
          val schemaLoader: SchemaLoader[SoQLType] = if(actuallyDoDataOps) backup.schemaLoader else noopSchemaLoader

          schemaLoader.create(copy)

          val schema = locally {
            val createdColumns = new MutableColumnIdMap[ColumnInfo[SoQLType]]
            for(col0 <- columns) {
              val col1 = datasetMap.addColumnWithId(col0.systemId, copy, col0.logicalName, typeNamespace.typeForName(copy.datasetInfo, col0.typeName), col0.physicalColumnBaseBase)
              schemaLoader.addColumn(col1)

              def make[CT](col: ColumnInfo[CT])(cond: Boolean, mapOp: (ColumnInfo[CT]) => ColumnInfo[CT], op: (ColumnInfo[CT]) => Unit): ColumnInfo[CT] = {
                if(cond) {
                  val newCol = mapOp(col)
                  op(newCol)
                  newCol
                } else {
                  col
                }
              }
              val col2 = make(col1)(col0.isSystemPrimaryKey, datasetMap.setSystemPrimaryKey, schemaLoader.makeSystemPrimaryKey)
              val col3 = make(col2)(col0.isUserPrimaryKey, datasetMap.setUserPrimaryKey, schemaLoader.makePrimaryKey)
              val col4 = make(col3)(col0.isVersion, datasetMap.setVersion, schemaLoader.makeVersion)
              val colLast = col4

              createdColumns(col4.systemId) = colLast
            }
            createdColumns.freeze()
          }

          if(actuallyDoDataOps) copyDataForResync(backup, client)(copy, schema, columns.map(_.systemId))

          loop()
        case Some(NoMoreCopies()) =>
          // done
        case Some(_) =>
          ??? // TODO: Unexpected packet
        case None =>
          ??? // TODO: EOF
      }
    }
    loop()

    conn.commit()
    client.send(ResyncComplete())
  }

  def copyDataForResync(backup: Backup, client: Packets)(copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo[SoQLType]], columns: Seq[ColumnId]) {
    val decsvifier = backup.decsvifier(copyInfo, schema)
    for {
      is <- managed(new PacketsInputStream(client, ResyncStreamDataLabel, ResyncStreamEndLabel, dataTimeout))
      decompressed <- managed(new SnappyInputStream(is))
      reader <- managed(new InputStreamReader(decompressed, "UTF-8"))
    } {
      decsvifier.importFromCsv(reader, columns)
    }
  }

  def noopSchemaLoader[CT] = new SchemaLoader[CT] {
    def create(copyInfo: CopyInfo) {}

    def drop(copyInfo: CopyInfo) {}

    def addColumn(colInfo: ColumnInfo[CT]) {}

    def dropColumn(colInfo: ColumnInfo[CT]) {}

    def makePrimaryKey(colInfo: ColumnInfo[CT]) {}

    def makeSystemPrimaryKey(colInfo: ColumnInfo[CT]) {}

    def makeVersion(colInfo: ColumnInfo[CT]) {}

    def dropPrimaryKey(colInfo: ColumnInfo[CT]): Boolean = true
  }
}

