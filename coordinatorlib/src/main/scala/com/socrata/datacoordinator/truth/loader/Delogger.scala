package com.socrata.datacoordinator
package truth.loader

import java.io.{ByteArrayInputStream, OutputStream, Closeable}

import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.datacoordinator.truth.metadata.{UnanchoredDatasetInfo, UnanchoredColumnInfo, UnanchoredCopyInfo}
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.truth.RowLogCodec
import scala.collection.immutable.VectorBuilder
import java.util.zip.InflaterInputStream

sealed abstract class CorruptLogException(val version: Long, msg: String) extends Exception(msg)
class MissingVersion(version: Long, msg: String) extends CorruptLogException(version, msg)
class NoEndOfTransactionMarker(version: Long, msg: String) extends CorruptLogException(version, msg)
class SkippedSubversion(version: Long, val expectedSubversion: Long, val foundSubversion: Long, msg: String) extends CorruptLogException(version, msg)
class UnknownEvent(version: Long, val event: String, msg: String) extends CorruptLogException(version, msg)

trait Delogger[CV] extends Closeable {
  @throws(classOf[MissingVersion])
  def delog(version: Long): CloseableIterator[Delogger.LogEvent[CV]]
  def findPublishEvent(fromVersion: Long, toVersion: Long): Option[Long]
  def lastWorkingCopyCreatedVersion: Option[Long]
  def lastWorkingCopyDroppedOrPublishedVersion: Option[Long]
  def lastVersion: Option[Long]
}

object Delogger {
  sealed trait LogEventCompanion

  sealed abstract class LogEvent[+CV] extends Product {
    def companion = companionFromProductName(productPrefix)
  }
  object LogEvent {
    def fromProductName(s: String): LogEventCompanion = companionFromProductName(s)
  }

  case object Truncated extends LogEvent[Nothing] with LogEventCompanion

  case class ColumnCreated(info: UnanchoredColumnInfo) extends LogEvent[Nothing]
  object ColumnCreated extends LogEventCompanion

  case class ColumnRemoved(info: UnanchoredColumnInfo) extends LogEvent[Nothing]
  object ColumnRemoved extends LogEventCompanion

  case class RowIdentifierSet(info: UnanchoredColumnInfo) extends LogEvent[Nothing]
  object RowIdentifierSet extends LogEventCompanion

  case class RowIdentifierCleared(info: UnanchoredColumnInfo) extends LogEvent[Nothing]
  object RowIdentifierCleared extends LogEventCompanion

  case class SystemRowIdentifierChanged(info: UnanchoredColumnInfo) extends LogEvent[Nothing]
  object SystemRowIdentifierChanged extends LogEventCompanion

  case class VersionColumnChanged(info: UnanchoredColumnInfo) extends LogEvent[Nothing]
  object VersionColumnChanged extends LogEventCompanion

  case class WorkingCopyCreated(datasetInfo: UnanchoredDatasetInfo, copyInfo: UnanchoredCopyInfo) extends LogEvent[Nothing]
  object WorkingCopyCreated extends LogEventCompanion

  case object WorkingCopyDropped extends LogEvent[Nothing] with LogEventCompanion

  case object DataCopied extends LogEvent[Nothing] with LogEventCompanion

  case class SnapshotDropped(info: UnanchoredCopyInfo) extends LogEvent[Nothing]
  object SnapshotDropped extends LogEventCompanion

  case object WorkingCopyPublished extends LogEvent[Nothing] with LogEventCompanion

  case class CounterUpdated(nextCounter: Long) extends LogEvent[Nothing]
  object CounterUpdated extends LogEventCompanion

  case object EndTransaction extends LogEvent[Nothing] with LogEventCompanion

  case class ColumnLogicalNameChanged(info: UnanchoredColumnInfo) extends LogEvent[Nothing]
  object ColumnLogicalNameChanged extends LogEventCompanion

  case class RowDataUpdated[CV](bytes: Array[Byte])(codec: RowLogCodec[CV]) extends LogEvent[CV] {
    lazy val operations: Vector[Operation[CV]] = { // TODO: A standard decode exception
      val bais = new ByteArrayInputStream(bytes)
      val underlyingInputStream = bais.read() match {
        case 0 => // ok, we're using Snappy
          new org.xerial.snappy.SnappyInputStream(bais)
        case 1 => // no compression
          bais
        case 2 => // deflate
          new InflaterInputStream(bais)
        case -1 => sys.error("Empty row data")
        case other => sys.error("Using an unknown compressiong format " + other)
      }
      val cis = com.google.protobuf.CodedInputStream.newInstance(underlyingInputStream)

      // TODO: dispatch on version (right now we have only one)
      codec.skipVersion(cis)

      val results = new VectorBuilder[Operation[CV]]
      def loop(): Vector[Operation[CV]] = {
        codec.extract(cis) match {
          case Some(op) =>
            results += op
            loop()
          case None =>
            results.result()
        }
      }

      loop()
    }
  }
  object RowDataUpdated extends LogEventCompanion

  // Note: the Delogger test checks that this is exhaustive
  val allLogEventCompanions: Set[LogEventCompanion] =
    Set(Truncated, ColumnCreated, ColumnRemoved, RowIdentifierSet, RowIdentifierCleared,
      SystemRowIdentifierChanged, VersionColumnChanged, WorkingCopyCreated, DataCopied, WorkingCopyPublished,
      WorkingCopyDropped, SnapshotDropped, ColumnLogicalNameChanged, RowDataUpdated, CounterUpdated, EndTransaction)

  // Note: the Delogger test checks that this is exhaustive.  It is not intended
  // to be used outside of this object and that test.
  private[loader] val companionFromProductName =
    allLogEventCompanions.foldLeft(Map.empty[String, LogEventCompanion]) { (acc, obj) =>
      val n = obj match {
        case Truncated => "Truncated"
        case ColumnCreated => "ColumnCreated"
        case ColumnRemoved => "ColumnRemoved"
        case RowIdentifierSet => "RowIdentifierSet"
        case RowIdentifierCleared => "RowIdentifierCleared"
        case SystemRowIdentifierChanged => "SystemRowIdentifierChanged"
        case VersionColumnChanged => "VersionColumnChanged"
        case WorkingCopyCreated => "WorkingCopyCreated"
        case DataCopied => "DataCopied"
        case WorkingCopyPublished => "WorkingCopyPublished"
        case WorkingCopyDropped => "WorkingCopyDropped"
        case SnapshotDropped => "SnapshotDropped"
        case ColumnLogicalNameChanged => "ColumnLogicalNameChanged"
        case RowDataUpdated => "RowDataUpdated"
        case CounterUpdated => "CounterUpdated"
        case EndTransaction => "EndTransaction"
      }
      acc + (n -> obj)
    }


  // Note: This is not intended to be used outside of this object and that test.
  private[loader] final val productNameFromCompanion =
    companionFromProductName.foldLeft(Map.empty[LogEventCompanion, String]) { (acc, kv) =>
      acc + kv.swap
    }
}
