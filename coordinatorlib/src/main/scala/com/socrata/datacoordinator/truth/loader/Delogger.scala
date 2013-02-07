package com.socrata.datacoordinator
package truth.loader

import java.io.{ByteArrayInputStream, OutputStream, Closeable}

import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.truth.RowLogCodec
import scala.collection.immutable.VectorBuilder

trait Delogger[CV] extends Closeable {
  def delog(version: Long): CloseableIterator[Delogger.LogEvent[CV]]
}

object Delogger {
  sealed abstract class LogEvent[+CV] extends Product
  case object Truncated extends LogEvent[Nothing]
  case class ColumnCreated(info: ColumnInfo) extends LogEvent[Nothing]
  case class ColumnRemoved(info: ColumnInfo) extends LogEvent[Nothing]
  case class RowIdentifierSet(info: ColumnInfo) extends LogEvent[Nothing]
  case class RowIdentifierCleared(info: ColumnInfo) extends LogEvent[Nothing]
  case class SystemRowIdentifierChanged(info: ColumnInfo) extends LogEvent[Nothing]
  case class WorkingCopyCreated(copyInfo: CopyInfo) extends LogEvent[Nothing]
  case object DataCopied extends LogEvent[Nothing]
  case object WorkingCopyDropped extends LogEvent[Nothing]
  case object WorkingCopyPublished extends LogEvent[Nothing]
  case class RowIdCounterUpdated(nextRowId: RowId) extends LogEvent[Nothing]
  case object EndTransaction extends LogEvent[Nothing]

  case class RowDataUpdated[CV](bytes: Array[Byte])(codec: RowLogCodec[CV]) extends LogEvent[CV] {
    lazy val operations: Vector[Operation[CV]] = { // TODO: A standard decode exception
      val bais = new ByteArrayInputStream(bytes)
      val sis = new org.xerial.snappy.SnappyInputStream(bais)
      val cis = com.google.protobuf.CodedInputStream.newInstance(sis)

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
}
