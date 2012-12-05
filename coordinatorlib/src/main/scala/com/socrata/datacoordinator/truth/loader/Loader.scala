package com.socrata.datacoordinator
package truth.loader

import scala.{collection => sc}

import java.io.Closeable

trait Loader[CV] extends Closeable {
  def upsert(row: Row[CV])
  def delete(id: CV)

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

/** A report is a collection of maps that inform the caller what
  * the result of each operation performed on the transaction so far was.
  *
  * @note Performing more operations after generating a report may mutate
  *       the maps returned in the original report.  The intent of it is to
  *       be called immediately before `commit()`.
  */
trait Report[CV] {
  /** Map from job number to the identifier of the row that was created. */
  def inserted: sc.Map[Int, CV]

  /** Map from job number to the identifier of the row that was updated. */
  def updated: sc.Map[Int, CV]

  /** Map from job number to the identifier of the row that was deleted. */
  def deleted: sc.Map[Int, CV]

  /** Map from job number to the identifier of the row that was merged with another job, and the job it was merged with. */
  def elided: sc.Map[Int, (CV, Int)]

  /** Map from job number to a value explaining the cause of the problem.. */
  def errors: sc.Map[Int, Failure[CV]]
}

sealed abstract class Failure[+CV]
case object NullPrimaryKey extends Failure[Nothing]
case class SystemColumnsSet(names: Set[String]) extends Failure[Nothing]
case class NoSuchRowToDelete[CV](id: CV) extends Failure[CV]
case class NoSuchRowToUpdate[CV](id: CV) extends Failure[CV]
case object NoPrimaryKey extends Failure[Nothing]

