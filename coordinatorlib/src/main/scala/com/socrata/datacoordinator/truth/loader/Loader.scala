package com.socrata.datacoordinator
package truth.loader

import scala.{collection => sc}

import java.io.Closeable
import com.socrata.datacoordinator.id.RowVersion

trait Loader[CV] extends Closeable {
  def upsert(jobId: Int, row: Row[CV], bySystemIdForced: Boolean):Unit
  def delete(jobId: Int, id: CV, version: Option[Option[RowVersion]], bySystemIdForced: Boolean): Unit

  /** Flushes any changes which have accumulated in-memory.
    *
    * @note This should be just about the last method on this interface called
    *       before closing it; calling either `upsert` or `delete` after this
    *       results in undefined behaviour.
    * @note If any changes were made and this method is not called, the database
    *       transaction MUST be rolled back.
    */
  def finish(): Unit
}

case class IdAndVersion[+CV](id: CV, version: RowVersion, bySystemIdForced: Boolean)
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
  val allFailures = Map[String, Class[_ <: Failure[_]]](
    "version_on_new_row" -> VersionOnNewRow.getClass.asInstanceOf[Class[VersionOnNewRow.type]],
    "no_such_row_to_delete" -> classOf[NoSuchRowToDelete[_]],
    "no_such_row_to_update" -> classOf[NoSuchRowToUpdate[_]],
    "no_primary_key" -> NoPrimaryKey.getClass.asInstanceOf[Class[NoPrimaryKey.type]],
    "verison_mismatch" -> classOf[VersionMismatch[_]], // FIXME: remove this one once we're sure no one is using it
    "version_mismatch" -> classOf[VersionMismatch[_]],
    "insert_in_update_only" -> classOf[InsertInUpdateOnly[_]]
  )
}

case object VersionOnNewRow extends Failure[Nothing] {
  def map[B](f: Nothing => B) = this
}
case class NoSuchRowToDelete[CV](id: CV) extends Failure[CV] {
  def map[B](f: CV => B) = NoSuchRowToDelete(f(id))
}
case class NoSuchRowToUpdate[CV](id: CV, bySystemIdForced: Boolean) extends Failure[CV] {
  def map[B](f: CV => B) = NoSuchRowToUpdate(f(id), bySystemIdForced)
}
case object NoPrimaryKey extends Failure[Nothing] {
  def map[B](f: Nothing => B) = this
}
case class VersionMismatch[CV](id: CV, expected: Option[RowVersion], actual: Option[RowVersion], bySystemIdForced: Boolean) extends Failure[CV] {
  def map[B](f: CV => B) = VersionMismatch(f(id), expected, actual, bySystemIdForced)
}
case class InsertInUpdateOnly[CV](id: CV, bySystemIdForced: Boolean) extends Failure[CV] {
  def map[B](f: CV => B) = InsertInUpdateOnly(f(id), bySystemIdForced)
}