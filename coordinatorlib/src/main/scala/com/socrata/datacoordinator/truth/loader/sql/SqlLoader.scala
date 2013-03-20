package com.socrata.datacoordinator
package truth.loader
package sql

import scala.{collection => sc}

import java.sql.Connection
import java.util.concurrent.Executor

import gnu.trove.map.hash.TIntObjectHashMap

import com.socrata.id.numeric.IdProvider

import com.socrata.datacoordinator.util.{RowIdProvider, TimingReport, TIntObjectHashMapWrapper, Counter}

/**
 * @note After passing the `dataLogger` to this constructor, the created `SqlLoader`
 *       should be considered to own it until `report` or `close` are called.  Until
 *       that point, it may be accessed by another thread.
 */
abstract class SqlLoader[CT, CV](val connection: Connection,
                                 val rowPreparer: RowPreparer[CV],
                                 val sqlizer: DataSqlizer[CT, CV],
                                 val dataLogger: DataLogger[CV],
                                 val idProvider: RowIdProvider,
                                 val executor: Executor,
                                 val timingReport: TimingReport)
  extends Loader[CV]
{
  require(!connection.getAutoCommit, "Connection is in auto-commit mode")

  val typeContext = sqlizer.typeContext
  val datasetContext = sqlizer.datasetContext

  val softMaxBatchSizeInBytes = sqlizer.softMaxBatchSize

  val inserted = new TIntObjectHashMap[CV]
  val elided = new TIntObjectHashMap[(CV, Int)]
  val updated = new TIntObjectHashMap[CV]
  val deleted = new TIntObjectHashMap[CV]
  val errors = new TIntObjectHashMap[Failure[CV]]

  protected val connectionMutex = new Object
  def checkAsyncJob()

  val nextJobNum = new Counter

  def flush()

  def report: Report[CV] = {
    flush()

    connectionMutex.synchronized {
      checkAsyncJob()

      def w[T](x: TIntObjectHashMap[T]) = TIntObjectHashMapWrapper(x)
      new SqlLoader.JobReport(w(inserted), w(updated), w(deleted), w(elided), w(errors))
    }
  }

  def close() {
    connectionMutex.synchronized {
      checkAsyncJob()
    }
  }
}

object SqlLoader {
  def apply[CT, CV](connection: Connection, preparer: RowPreparer[CV], sqlizer: DataSqlizer[CT, CV], dataLogger: DataLogger[CV], idProvider: RowIdProvider, executor: Executor, timingReport: TimingReport): SqlLoader[CT,CV] = {
    if(sqlizer.datasetContext.hasUserPrimaryKey)
      new UserPKSqlLoader(connection, preparer, sqlizer, dataLogger, idProvider, executor, timingReport)
    else
      new SystemPKSqlLoader(connection, preparer, sqlizer, dataLogger, idProvider, executor, timingReport)
  }

  case class JobReport[CV](inserted: sc.Map[Int, CV], updated: sc.Map[Int, CV], deleted: sc.Map[Int, CV], elided: sc.Map[Int, (CV, Int)], errors: sc.Map[Int, Failure[CV]]) extends Report[CV]
}
