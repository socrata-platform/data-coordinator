package com.socrata.datacoordinator
package truth.loader
package sql

import scala.{collection => sc}

import java.sql.{Connection, PreparedStatement}
import java.util.concurrent.Executor

import com.rojoma.simplearm.util._
import gnu.trove.map.hash.TIntObjectHashMap
import com.socrata.datacoordinator.util.{TIntObjectHashMapWrapper, Counter, IdProviderPool}

abstract class SqlLoader[CT, CV](val connection: Connection,
                                 val typeContext: TypeContext[CV],
                                 val sqlizer: DataSqlizer[CT, CV],
                                 val idProviderPool: IdProviderPool,
                                 val executor: Executor)
  extends Loader[CV]
{
  require(!connection.getAutoCommit, "Connection is in auto-commit mode")

  private val log = SqlLoader.log

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

  lazy val versionNum = for {
    stmt <- managed(connection.createStatement())
    rs <- managed(stmt.executeQuery(sqlizer.findCurrentVersion))
  } yield {
    val hasNext = rs.next()
    assert(hasNext, "next version query didn't return anything?")
    rs.getLong(1) + 1
  }

  val nextSubVersionNum = new Counter(init = 1)

  object rowAuxDataState extends (sqlizer.LogAuxColumn => Unit) {
    var stmt: PreparedStatement = null

    var batched = 0
    var size = 0

    def apply(auxData: sqlizer.LogAuxColumn) {
      if(stmt == null) stmt = connection.prepareStatement(sqlizer.prepareLogRowsChangedStatement)
      size += sqlizer.prepareLogRowsChanged(stmt, versionNum, nextSubVersionNum(), auxData)
      stmt.addBatch()
      batched += 1
      if(size > sqlizer.softMaxBatchSize) flush()
    }

    def flush() {
      if(batched != 0) {
        log.debug("Flushing {} log rows", batched)
        val rs = stmt.executeBatch()
        assert(rs.length == batched)
        assert(rs.forall(_ == 1), "Inserting a log row... didn't insert a log row?")
        batched = 0
        size = 0
      }
    }

    def close() {
      if(stmt != null) stmt.close()
    }
  }
  val rowAuxData = sqlizer.newRowAuxDataAccumulator(rowAuxDataState)

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

      rowAuxData.finish()
      rowAuxDataState.flush()

      implicit def wrap[T](x: TIntObjectHashMap[T]) = TIntObjectHashMapWrapper(x)
      new SqlLoader.JobReport(versionNum, inserted, updated, deleted, elided, errors)
    }
  }

  def close() {
    connectionMutex.synchronized {
      try {
        checkAsyncJob()
      } finally {
        try {
          rowAuxDataState.close()
        } finally {
          idProviderPool.release(idProvider)
        }
      }
    }
  }
}

object SqlLoader {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[SqlLoader[_, _]])

  def apply[CT, CV](connection: Connection, typeContext: TypeContext[CV], sqlizer: DataSqlizer[CT, CV], idProvider: IdProviderPool, executor: Executor): SqlLoader[CT,CV] = {
    if(sqlizer.datasetContext.hasUserPrimaryKey)
      new UserPKSqlLoader(connection, typeContext, sqlizer, idProvider, executor)
    else
      new SystemPKSqlLoader(connection, typeContext, sqlizer, idProvider, executor)
  }

  case class JobReport[CV](newVersion: Long, inserted: sc.Map[Int, CV], updated: sc.Map[Int, CV], deleted: sc.Map[Int, CV], elided: sc.Map[Int, (CV, Int)], errors: sc.Map[Int, Failure[CV]]) extends Report[CV]
}
