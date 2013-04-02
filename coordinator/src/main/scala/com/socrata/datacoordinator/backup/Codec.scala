package com.socrata.datacoordinator.backup

import java.io.{InputStreamReader, InputStream, DataInputStream, DataOutputStream}

import com.rojoma.json.util.JsonUtil

import com.socrata.datacoordinator.truth.loader.Delogger
import com.socrata.datacoordinator.truth.loader.Delogger.{LogEventCompanion, LogEvent}
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.id.{RowIdProcessor, RowId}
import com.socrata.datacoordinator.truth.RowLogCodec

trait Codec[T] {
  def encode(target: DataOutputStream, data: T)
  def decode(input: DataInputStream, rowIdProcessor: RowIdProcessor): T
}

class LogDataCodec[CV](rowLogCodecFactory: () => RowLogCodec[CV]) extends Codec[Delogger.LogEvent[CV]] {
  def encode(dos: DataOutputStream, event: LogEvent[CV]) {
    LogDataCodec.encodeEvent(dos, event)
  }

  def decode(stream: DataInputStream, rowIdProcessor: RowIdProcessor): Delogger.LogEvent[CV] = {
    LogDataCodec.decodeEvent(Delogger.LogEvent.fromProductName(eventType(stream)), stream, rowLogCodecFactory, rowIdProcessor)
  }

  def eventType(in: InputStream) = {
    val sb = new java.lang.StringBuilder
    def loop() {
      in.read() match {
        case -1 => throw new PacketDecodeException("LogData packet truncated before the event type")
        case 0 => // done
        case c => sb.append(c.toChar); loop()
      }
    }
    loop()
    sb.toString
  }
}

object LogDataCodec {
  def encodeEvent(stream: DataOutputStream, event: Delogger.LogEvent[_]) {
    stream.write(event.productPrefix.getBytes)
    stream.write(0)
    eventMap(event.companion).encode(stream, event)
  }

  def decodeEvent[CV](eventType: LogEventCompanion, stream: DataInputStream, rowLogCodecFactory: () => RowLogCodec[CV], rowIdProcessor: RowIdProcessor) =
    eventMap(eventType).decode(stream, rowLogCodecFactory, rowIdProcessor)

  private abstract class EventCodec {
    def encode(stream: DataOutputStream, event: Delogger.LogEvent[Any])
    def decode[CV](stream: DataInputStream, rowCodecFactory: () => RowLogCodec[CV], rowIdProcessor: RowIdProcessor): Delogger.LogEvent[CV]
  }

  private val eventMap = Map[LogEventCompanion, EventCodec](
    Delogger.RowDataUpdated -> RowDataUpdatedCodec,
    Delogger.RowIdCounterUpdated -> RowIdCounterUpdatedCodec,
    Delogger.WorkingCopyCreated -> WorkingCopyCreatedCodec,
    Delogger.WorkingCopyPublished -> WorkingCopyPublishedCodec,
    Delogger.DataCopied -> DataCopiedCodec,
    Delogger.Truncated -> TruncatedCodec,
    Delogger.WorkingCopyDropped -> WorkingCopyDroppedCodec,
    Delogger.SnapshotDropped -> SnapshotDroppedCodec,
    Delogger.ColumnCreated -> ColumnCreatedCodec,
    Delogger.RowIdentifierSet -> RowIdentifierSetCodec,
    Delogger.RowIdentifierCleared -> RowIdentifierClearedCodec,
    Delogger.ColumnRemoved -> ColumnRemovedCodec,
    Delogger.SystemRowIdentifierChanged -> SystemRowIdentifierChangedCodec,
    Delogger.ColumnLogicalNameChanged-> ColumnLogicalNameChangedCodec,
    Delogger.EndTransaction -> EndTransactionCodec
  )
  assert(eventMap.size == Delogger.allLogEventCompanions.size,
    "Missing decoders for " + (Delogger.allLogEventCompanions -- eventMap.keySet))

  private object RowDataUpdatedCodec extends EventCodec {
    def encode(stream: DataOutputStream, eventRaw: Delogger.LogEvent[Any]) {
      val Delogger.RowDataUpdated(bytes) = eventRaw
      stream.writeInt(bytes.length)
      stream.write(bytes)
    }

    def decode[CV](stream: DataInputStream, rowLogCodecFactory: () => RowLogCodec[CV], rowIdProcessor: RowIdProcessor) = {
      val count = stream.readInt()
      val bytes = new Array[Byte](count)
      stream.readFully(bytes)
      Delogger.RowDataUpdated(bytes)(rowLogCodecFactory(), rowIdProcessor)
    }
  }

  private object RowIdCounterUpdatedCodec extends EventCodec {
    def encode(stream: DataOutputStream, eventRaw: Delogger.LogEvent[Any]) {
      val Delogger.RowIdCounterUpdated(rid) = eventRaw
      stream.writeLong(rid.numeric)
    }

    def decode[CV](stream: DataInputStream, rowLogCodecFactory: () => RowLogCodec[CV], rowIdProcessor: RowIdProcessor) = {
      val rid = rowIdProcessor(stream.readLong())
      Delogger.RowIdCounterUpdated(rid)
    }
  }

  private object WorkingCopyCreatedCodec extends EventCodec {
    def encode(stream: DataOutputStream, eventRaw: Delogger.LogEvent[Any]) {
      val Delogger.WorkingCopyCreated(di, ci) = eventRaw
      stream.write(JsonUtil.renderJson(di).getBytes("UTF-8"))
      stream.write('\n')
      stream.write(JsonUtil.renderJson(ci).getBytes("UTF-8"))
    }

    def decode[CV](stream: DataInputStream, rowLogCodecFactory: () => RowLogCodec[CV], rowIdProcessor: RowIdProcessor) = {
      val r = new InputStreamReader(stream, "UTF-8")
      val di = JsonUtil.readJson[UnanchoredDatasetInfo](r).getOrElse {
        throw new PacketDecodeException("Unable to decode a datasetinfo")
      }
      val ci = JsonUtil.readJson[UnanchoredCopyInfo](r).getOrElse {
        throw new PacketDecodeException("Unable to decode a copyinfo")
      }
      Delogger.WorkingCopyCreated(di, ci)
    }
  }

  private object WorkingCopyPublishedCodec extends EventCodec {
    def encode(stream: DataOutputStream, event: Delogger.LogEvent[Any]) {
      val Delogger.WorkingCopyPublished = event
    }
    def decode[CV](stream: DataInputStream, rowCodecFactory: () => RowLogCodec[CV], rowIdProcessor: RowIdProcessor) =
      Delogger.WorkingCopyPublished
  }

  private object DataCopiedCodec extends EventCodec {
    def encode(stream: DataOutputStream, event: Delogger.LogEvent[Any]) {
      val Delogger.DataCopied = event
    }
    def decode[CV](stream: DataInputStream, rowCodecFactory: () => RowLogCodec[CV], rowIdProcessor: RowIdProcessor) =
      Delogger.DataCopied
  }

  private object TruncatedCodec extends EventCodec {
    def encode(stream: DataOutputStream, event: Delogger.LogEvent[Any]) {
      val Delogger.Truncated = event
    }
    def decode[CV](stream: DataInputStream, rowCodecFactory: () => RowLogCodec[CV], rowIdProcessor: RowIdProcessor) =
      Delogger.Truncated
  }

  private object WorkingCopyDroppedCodec extends EventCodec {
    def encode(stream: DataOutputStream, event: Delogger.LogEvent[Any]) {
      val Delogger.WorkingCopyDropped = event
    }
    def decode[CV](stream: DataInputStream, rowCodecFactory: () => RowLogCodec[CV], rowIdProcessor: RowIdProcessor) =
      Delogger.WorkingCopyDropped
  }

  private object SnapshotDroppedCodec extends EventCodec {
    def encode(stream: DataOutputStream, event: Delogger.LogEvent[Any]) {
      val Delogger.SnapshotDropped(ci) = event
      stream.write(JsonUtil.renderJson(ci).getBytes("UTF-8"))
    }
    def decode[CV](stream: DataInputStream, rowLogCodecFactory: () => RowLogCodec[CV], rowIdProcessor: RowIdProcessor) = {
      val ci = JsonUtil.readJson[UnanchoredCopyInfo](new InputStreamReader(stream, "UTF-8")).getOrElse {
        throw new PacketDecodeException("Unable to decode a columnInfo")
      }
      Delogger.SnapshotDropped(ci)
    }
  }

  private abstract class ColumnEventCodec extends EventCodec {
    def extractColumn(event: Delogger.LogEvent[Any]): UnanchoredColumnInfo
    def packageColumn(col: UnanchoredColumnInfo): Delogger.LogEvent[Nothing]

    def encode(stream: DataOutputStream, event: Delogger.LogEvent[Any]) {
      val col = extractColumn(event)
      stream.write(JsonUtil.renderJson(col).getBytes("UTF-8"))
    }

    def decode[CV](stream: DataInputStream, rowLogCodecFactory: () => RowLogCodec[CV], rowIdProcessor: RowIdProcessor) = {
      val ci = JsonUtil.readJson[UnanchoredColumnInfo](new InputStreamReader(stream, "UTF-8")).getOrElse {
        throw new PacketDecodeException("Unable to decode a columnInfo")
      }
      packageColumn(ci)
    }
  }

  private object ColumnCreatedCodec extends ColumnEventCodec {
    def extractColumn(event: Delogger.LogEvent[Any]): UnanchoredColumnInfo = {
      val Delogger.ColumnCreated(col) = event
      col
    }
    def packageColumn(col: UnanchoredColumnInfo): Delogger.LogEvent[Nothing] =
      Delogger.ColumnCreated(col)
  }

  private object RowIdentifierSetCodec extends ColumnEventCodec {
    def extractColumn(event: Delogger.LogEvent[Any]): UnanchoredColumnInfo = {
      val Delogger.RowIdentifierSet(col) = event
      col
    }
    def packageColumn(col: UnanchoredColumnInfo): Delogger.LogEvent[Nothing] =
      Delogger.RowIdentifierSet(col)
  }

  private object RowIdentifierClearedCodec extends ColumnEventCodec {
    def extractColumn(event: Delogger.LogEvent[Any]): UnanchoredColumnInfo = {
      val Delogger.RowIdentifierCleared(col) = event
      col
    }
    def packageColumn(col: UnanchoredColumnInfo): Delogger.LogEvent[Nothing] =
      Delogger.RowIdentifierCleared(col)
  }

  private object ColumnRemovedCodec extends ColumnEventCodec {
    def extractColumn(event: Delogger.LogEvent[Any]): UnanchoredColumnInfo = {
      val Delogger.ColumnRemoved(col) = event
      col
    }
    def packageColumn(col: UnanchoredColumnInfo): Delogger.LogEvent[Nothing] =
      Delogger.ColumnRemoved(col)
  }

  private object SystemRowIdentifierChangedCodec extends ColumnEventCodec {
    def extractColumn(event: Delogger.LogEvent[Any]): UnanchoredColumnInfo = {
      val Delogger.SystemRowIdentifierChanged(col) = event
      col
    }
    def packageColumn(col: UnanchoredColumnInfo): Delogger.LogEvent[Nothing] =
      Delogger.SystemRowIdentifierChanged(col)
  }

  private object ColumnLogicalNameChangedCodec extends ColumnEventCodec {
    def extractColumn(event: Delogger.LogEvent[Any]): UnanchoredColumnInfo = {
      val Delogger.ColumnLogicalNameChanged(col) = event
      col
    }
    def packageColumn(col: UnanchoredColumnInfo): Delogger.LogEvent[Nothing] =
      Delogger.ColumnLogicalNameChanged(col)
  }

  private object EndTransactionCodec extends EventCodec {
    def encode(stream: DataOutputStream, event: LogEvent[Any]) {
      val Delogger.EndTransaction = event
    }

    def decode[CV](stream: DataInputStream, rowCodecFactory: () => RowLogCodec[CV], rowIdProcessor: RowIdProcessor): LogEvent[CV] =
      Delogger.EndTransaction
  }
}
