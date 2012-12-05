package com.socrata.datacoordinator
package truth.loader
package sql

import scala.{collection => sc}

import java.sql.Connection
import java.util.concurrent.Executor

import gnu.trove.map.hash.TIntObjectHashMap

import com.socrata.datacoordinator.util.{TIntObjectHashMapWrapper, Counter, IdProviderPool}

/**
 * @note After passing the `dataLogger` to this constructor, the created `SqlLoader`
 *       should be considered to own it until `report` or `close` are called.  Until
 *       that point, it may be accessed by another thread.
 */
abstract class SqlLoader[CT, CV](val connection: Connection,
                                 val rowPreparer: RowPreparer[CV],
                                 val sqlizer: DataSqlizer[CT, CV],
                                 val dataLogger: DataLogger[CV],
                                 val idProviderPool: IdProviderPool,
                                 val executor: Executor)
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

  // Any further initializations after this borrow must take care to
  // clean up after themselves if they may throw!  In particular they
  // must either early-initialize or rollback the transaction and
  // return the id provider to the pool.
  val idProvider = idProviderPool.borrow()

  def flush()

  def report: Report[CV] = {
    flush()

    connectionMutex.synchronized {
      checkAsyncJob()

      implicit def wrap[T](x: TIntObjectHashMap[T]) = TIntObjectHashMapWrapper(x)
      new SqlLoader.JobReport(inserted, updated, deleted, elided, errors)
    }
  }

  def close() {
    connectionMutex.synchronized {
      try {
        checkAsyncJob()
      } finally {
        idProviderPool.release(idProvider)
      }
    }
  }
}

object SqlLoader {
  def apply[CT, CV](connection: Connection, preparer: RowPreparer[CV], sqlizer: DataSqlizer[CT, CV], dataLogger: DataLogger[CV], idProvider: IdProviderPool, executor: Executor): SqlLoader[CT,CV] = {
    if(sqlizer.datasetContext.hasUserPrimaryKey)
      new UserPKSqlLoader(connection, preparer, sqlizer, dataLogger, idProvider, executor)
    else
      new SystemPKSqlLoader(connection, preparer, sqlizer, dataLogger, idProvider, executor)
  }

  case class JobReport[CV](inserted: sc.Map[Int, CV], updated: sc.Map[Int, CV], deleted: sc.Map[Int, CV], elided: sc.Map[Int, (CV, Int)], errors: sc.Map[Int, Failure[CV]]) extends Report[CV]
}
