package com.socrata.datacoordinator
package truth.loader

import scala.{collection => sc}

import java.io.Closeable
import com.socrata.datacoordinator.util.collection.ColumnIdSet
import com.rojoma.json.ast.{JObject, JString, JValue}
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.util.{AutomaticJsonCodecBuilder, TagToValue, SimpleHierarchyCodecBuilder}
import com.socrata.datacoordinator.id.RowVersion

trait Loader[CV] extends Closeable {
  def upsert(jobId: Int, row: Row[CV])
  def delete(jobId: Int, id: CV, version: Option[RowVersion] = None)

  /** Flushes any changes which have accumulated in-memory and
    * returns a report summarizing the changes.
    *
    * @note This should be just about the last method on this interface called
    *       before closing it; calling either `upsert` or `delete` after this
    *       results in undefined behaviour.
    * @note If any changes were made and this method is not called, the database
    *       transaction MUST be rolled back.
    */
  def report: Report[CV]
}

case class IdAndVersion[CV](id: CV, version: RowVersion)
/** A report is a collection of maps that inform the caller what
  * the result of each operation performed on the transaction so far was.
  *
  * @note Performing more operations after generating a report may mutate
  *       the maps returned in the original report.  The intent of it is to
  *       be called immediately before `commit()`.
  */
trait Report[CV] {
  /** Map from job number to the identifier of the row that was created. */
  def inserted: sc.Map[Int, IdAndVersion[CV]]

  /** Map from job number to the identifier of the row that was updated. */
  def updated: sc.Map[Int, IdAndVersion[CV]]

  /** Map from job number to the identifier of the row that was deleted. */
  def deleted: sc.Map[Int, CV]

  /** Map from job number to a value explaining the cause of the problem.. */
  def errors: sc.Map[Int, Failure[CV]]
}

sealed abstract class Failure[+CV] {
  def map[B](f: CV => B): Failure[B]
}

object Failure {
  private val NoPK = JString("no_primary_key")
  private val NoSuchRowToDeleteTag = "no_such_row_to_delete"
  private val NoSuchRowToUpdateTag = "no_such_row_to_update"
  private val VersionMismatchTag = "version_mismatch"
  implicit def jCodec[CV](implicit cvCodec: JsonCodec[CV]): JsonCodec[Failure[CV]] = new JsonCodec[Failure[CV]] {
    implicit val noSuchRowToDeleteCodec = new JsonCodec[NoSuchRowToDelete[CV]] {
      def encode(x: NoSuchRowToDelete[CV]) = cvCodec.encode(x.id)
      def decode(x: JValue) = cvCodec.decode(x).map(NoSuchRowToDelete(_))
    }

    implicit val noSuchRowToUpdateCodec = new JsonCodec[NoSuchRowToUpdate[CV]] {
      def encode(x: NoSuchRowToUpdate[CV]) = cvCodec.encode(x.id)
      def decode(x: JValue) = cvCodec.decode(x).map(NoSuchRowToUpdate(_))
    }

    implicit val versionMismatchCodec = AutomaticJsonCodecBuilder[VersionMismatch[CV]]

    val cb = SimpleHierarchyCodecBuilder[Failure[CV]](TagToValue).
      branch[NoSuchRowToDelete[CV]](NoSuchRowToDeleteTag).
      branch[NoSuchRowToUpdate[CV]](NoSuchRowToUpdateTag).
      branch[VersionMismatch[CV]](VersionMismatchTag).
      build

    def encode(x: Failure[CV]) = x match {
      case NoPrimaryKey => NoPK
      case other => cb.encode(other)
    }
    def decode(x: JValue) = x match {
      case NoPK => Some(NoPrimaryKey)
      case other => cb.decode(other)
    }
  }
}

case object VersionOnNewRow extends Failure[Nothing] {
  def map[B](f: Nothing => B) = this
}
case class NoSuchRowToDelete[CV](id: CV) extends Failure[CV] {
  def map[B](f: CV => B) = NoSuchRowToDelete(f(id))
}
case class NoSuchRowToUpdate[CV](id: CV) extends Failure[CV] {
  def map[B](f: CV => B) = NoSuchRowToUpdate(f(id))
}
case object NoPrimaryKey extends Failure[Nothing] {
  def map[B](f: Nothing => B) = this
}
case class VersionMismatch[CV](id: CV, expected: Option[RowVersion], actual: Option[RowVersion]) extends Failure[CV] {
  def map[B](f: CV => B) = VersionMismatch(f(id), expected, actual)
}
