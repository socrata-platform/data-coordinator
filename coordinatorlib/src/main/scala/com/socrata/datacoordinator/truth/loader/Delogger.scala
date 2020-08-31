package com.socrata.datacoordinator
package truth.loader

import java.io.{ByteArrayInputStream, Closeable, OutputStream}

import com.socrata.datacoordinator.secondary.ComputationStrategyInfo
import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.datacoordinator.truth.metadata.{UnanchoredColumnInfo, UnanchoredCopyInfo, UnanchoredDatasetInfo, UnanchoredRollupInfo}
import com.socrata.datacoordinator.id.{ColumnId, RowId}
import com.socrata.datacoordinator.truth.RowLogCodec
import com.socrata.soql.environment.ColumnName

import scala.collection.immutable.VectorBuilder
import java.util.zip.InflaterInputStream

import com.rojoma.json.v3.ast.JObject
import org.joda.time.DateTime

sealed abstract class CorruptLogException(val version: Long, msg: String) extends Exception(msg)
class MissingVersion(version: Long, msg: String) extends CorruptLogException(version, msg)
class NoEndOfTransactionMarker(version: Long, msg: String) extends CorruptLogException(version, msg)
class SkippedSubversion(version: Long, val expectedSubversion: Long, val foundSubversion: Long, msg: String) extends CorruptLogException(version, msg)
class UnknownEvent(version: Long, val event: String, msg: String) extends CorruptLogException(version, msg)

trait Delogger[CV] extends Closeable {
  @throws(classOf[MissingVersion])
  def delog(version: Long): CloseableIterator[Delogger.LogEvent[CV]]
  def delogOnlyTypes(version: Long): CloseableIterator[Delogger.LogEventCompanion]
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

  case class ComputationStrategyCreated(info: UnanchoredColumnInfo) extends LogEvent[Nothing]
  object ComputationStrategyCreated extends LogEventCompanion

  case class ComputationStrategyRemoved(info: UnanchoredColumnInfo) extends LogEvent[Nothing]
  object ComputationStrategyRemoved extends LogEventCompanion

  case class FieldNameUpdated(info: UnanchoredColumnInfo) extends LogEvent[Nothing]
  object FieldNameUpdated extends LogEventCompanion

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

  case class LastModifiedChanged(lastModified: DateTime) extends LogEvent[Nothing]
  object LastModifiedChanged extends LogEventCompanion

  case object WorkingCopyDropped extends LogEvent[Nothing] with LogEventCompanion

  case object DataCopied extends LogEvent[Nothing] with LogEventCompanion

  // Snapshots are now a data-coordinator internal thing; for now, SnapshotDropped
  // will continue to be logged on publish and sent to the secondaries, but they
  // should start to not depend on it.  Eventually, this will vanish.
  case class SnapshotDropped(info: UnanchoredCopyInfo) extends LogEvent[Nothing]
  object SnapshotDropped extends LogEventCompanion

  case object WorkingCopyPublished extends LogEvent[Nothing] with LogEventCompanion

  case class CounterUpdated(nextCounter: Long) extends LogEvent[Nothing]
  object CounterUpdated extends LogEventCompanion

  case class RollupCreatedOrUpdated(info: UnanchoredRollupInfo) extends LogEvent[Nothing]
  object RollupCreatedOrUpdated extends LogEventCompanion

  case class RollupDropped(info: UnanchoredRollupInfo) extends LogEvent[Nothing]
  object RollupDropped extends LogEventCompanion

  case class RowsChangedPreview(rowsInserted: Long, rowsUpdated: Long, rowsDeleted: Long, truncated: Boolean) extends LogEvent[Nothing]
  object RowsChangedPreview extends LogEventCompanion

  case object SecondaryReindex extends LogEvent[Nothing] with LogEventCompanion

  case class SecondaryAddIndex(fieldName: ColumnName, directives: JObject) extends LogEvent[Nothing]
  object SecondaryAddIndex extends LogEventCompanion

  case class SecondaryDeleteIndex(fieldName: ColumnName) extends LogEvent[Nothing]
  object SecondaryDeleteIndex extends LogEventCompanion

  case object EndTransaction extends LogEvent[Nothing] with LogEventCompanion

  case class RowDataUpdated[CV](bytes: Array[Byte])(codec: RowLogCodec[CV]) extends LogEvent[CV] {
    override def toString = "RowDataUpdated(" + operations + ")"
    lazy val operations: Vector[Operation[CV]] = { // TODO: A standard decode exception
      val bais = new ByteArrayInputStream(bytes)
      val underlyingInputStream = bais.read() match {
        case 0 => // ok, we're using Snappy
          new org.xerial.snappy.SnappyInputStream(bais)
        case 1 => // no compression
          bais
        case 2 => // deflate
          new InflaterInputStream(bais)
        case 3 => // pure java Snappy
          new org.iq80.snappy.SnappyInputStream(bais)
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
    Set(Truncated, ColumnCreated, ColumnRemoved, FieldNameUpdated, RowIdentifierSet, RowIdentifierCleared,
      ComputationStrategyCreated, ComputationStrategyRemoved,
      SystemRowIdentifierChanged, VersionColumnChanged, LastModifiedChanged, WorkingCopyCreated, DataCopied,
      WorkingCopyPublished, WorkingCopyDropped, SnapshotDropped, RowDataUpdated, CounterUpdated,
      RollupCreatedOrUpdated, RollupDropped, RowsChangedPreview,
      SecondaryReindex, SecondaryAddIndex, SecondaryDeleteIndex,
      EndTransaction)

  // Note: the Delogger test checks that this is exhaustive.  It is not intended
  // to be used outside of this object and that test.
  private[loader] val companionFromProductName =
    allLogEventCompanions.foldLeft(Map.empty[String, LogEventCompanion]) { (acc, obj) =>
      val n = obj match {
        case Truncated => "Truncated"
        case ColumnCreated => "ColumnCreated"
        case ColumnRemoved => "ColumnRemoved"
        case FieldNameUpdated => "FieldNameUpdated"
        case ComputationStrategyCreated => "ComputationStrategyCreated"
        case ComputationStrategyRemoved => "ComputationStrategyRemoved"
        case RowIdentifierSet => "RowIdentifierSet"
        case RowIdentifierCleared => "RowIdentifierCleared"
        case SystemRowIdentifierChanged => "SystemRowIdentifierChanged"
        case VersionColumnChanged => "VersionColumnChanged"
        case LastModifiedChanged => "LastModifiedChanged"
        case WorkingCopyCreated => "WorkingCopyCreated"
        case DataCopied => "DataCopied"
        case WorkingCopyPublished => "WorkingCopyPublished"
        case WorkingCopyDropped => "WorkingCopyDropped"
        case SnapshotDropped => "SnapshotDropped"
        case RowDataUpdated => "RowDataUpdated"
        case CounterUpdated => "CounterUpdated"
        case RollupCreatedOrUpdated => "RollupCreatedOrUpdated"
        case RollupDropped => "RollupDropped"
        case RowsChangedPreview => "RowsChangedPreview"
        case SecondaryReindex => "SecondaryReindex"
        case SecondaryAddIndex => "SecondaryAddIndex"
        case SecondaryDeleteIndex => "SecondaryDeleteIndex"
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
