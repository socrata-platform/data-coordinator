package com.socrata.datacoordinator.truth.loader

import scala.{collection => sc}
import gnu.trove.map.hash.TIntObjectHashMap
import com.socrata.datacoordinator.util.TIntObjectHashMapWrapper

/** Accumulate a report from a `Loader`.  These
  * reports can be large; this is provided to allow
  * them to be spooled to disk somewhere.
  *
  * @note These must safe to access concurrently
  *       from multiple threads before the loader's
  *       `finishe()` method is called.
  */
trait ReportWriter[-CV] {
  def inserted(job: Int, result: IdAndVersion[CV]): Unit
  def updated(job: Int, result: IdAndVersion[CV]): Unit
  def deleted(job: Int, result: CV): Unit
  def error(job: Int, result: Failure[CV]): Unit

  var finished: Boolean = false
}

class SimpleReportWriter[CV] extends ReportWriter[CV] {
  private val insertedMap = new TIntObjectHashMap[IdAndVersion[CV]]
  private val updatedMap = new TIntObjectHashMap[IdAndVersion[CV]]
  private val deletedMap = new TIntObjectHashMap[CV]
  private val errorMap = new TIntObjectHashMap[Failure[CV]]

  def inserted(job: Int, result: IdAndVersion[CV]) {
    insertedMap.synchronized { insertedMap.put(job, result) }
  }

  def updated(job: Int, result: IdAndVersion[CV]) {
    updatedMap.synchronized { updatedMap.put(job, result) }
  }

  def deleted(job: Int, result: CV) {
    deletedMap.synchronized { deletedMap.put(job, result) }
  }

  def error(job: Int, result: Failure[CV]) {
    errorMap.synchronized { errorMap.put(job, result) }
  }

  /* This must be called only after the loader is `finish()`ed */
  def report: Report[CV] = {
    assert(finished, "report() called without being finished first")
    def w[T](x: TIntObjectHashMap[T]) = TIntObjectHashMapWrapper(x)
    JobReport(w(insertedMap), w(updatedMap), w(deletedMap), w(errorMap))
  }

  private case class JobReport(inserted: sc.Map[Int, IdAndVersion[CV]], updated: sc.Map[Int, IdAndVersion[CV]], deleted: sc.Map[Int, CV], errors: sc.Map[Int, Failure[CV]]) extends Report[CV]
}

object NoopReportWriter extends ReportWriter[Any] {
  def inserted(job: Int, result: IdAndVersion[Any]) {}
  def updated(job: Int, result: IdAndVersion[Any]) {}
  def deleted(job: Int, result: Any) {}
  def error(job: Int, result: Failure[Any]) {}
}
