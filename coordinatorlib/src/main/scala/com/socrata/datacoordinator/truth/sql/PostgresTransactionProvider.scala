package com.socrata.datacoordinator
package truth.sql

import java.sql.Connection
import javax.sql.DataSource

import com.rojoma.simplearm.util._

import truth.{DatasetIdInUseByWriterException, DatasetInUseByWriterException}
import com.socrata.datacoordinator.truth.metadata.DatasetMapWriter
import com.socrata.datacoordinator.util.{LockProvider, LockTimeoutException}

final class PostgresTransactionProvider(lockProvider: LockProvider,
                                        mapFactory: Connection => DatasetMapWriter,
                                        lockTimeout: Long, snapshotRetryCount: Int, dataSource: DataSource) extends TransactionProvider {
  def lockNameFor(datasetId: String) = "write-dataset-" + datasetId

  def withReadTransaction[T](datasetId: String)(f: ConnectionInfo => T): T = {
    using(dataSource.getConnection()) { conn =>
      conn.setAutoCommit(false)
      conn.setReadOnly(true)
      val connInfo = new ConnectionInfo {
        val connection = conn
        val datasetMap = mapFactory(connection)
        val datasetInfo = datasetMap.datasetInfo(datasetId)
      }
      f(connInfo)
    }
  }

  def withWriteTransaction[T](datasetId: String)(f: ConnectionInfo => T): T =
    withWriteTransactionRemaining(datasetId, System.nanoTime + (lockTimeout * 1000000), f)

  def withWriteTransactionRemaining[T](datasetId: String, deadline: Long, f: ConnectionInfo => T): T = {
    while(true) {
      val databaseWaiter = try {
        val lock =
          try { lockProvider.lock(lockNameFor(datasetId), math.max(1L, (deadline - System.nanoTime()) / 1000000L)) }
          catch { case e: LockTimeoutException => throw new DatasetIdInUseByWriterException(datasetId, e) }
        try {
          using(dataSource.getConnection()) { conn =>
            conn.setAutoCommit(false)
            val connInfo = try {
              new ConnectionInfo {
                val connection = conn
                val datasetMap = mapFactory(connection)
                val datasetInfo = datasetMap.datasetInfo(datasetId)
              }
            } catch {
              case _: DatasetInUseByWriterException =>
                throw new Retry
            }
            return f(connInfo)
          }
        } catch {
          case _: Retry =>
            // That shouldn't have happened!  Whoever held the lockProvider lock lost it.
            // What we need to do, then, is start up a new thread which will hold the lock
            // whilst simultaneously trying to take the postgres lock in a _blocking_ manner.
            // Then we retry with the lock.
            //
            // I'd really rather pass my lock ownership to the new thread than have it take
            // a new one, but meh.
            val thread = new LockThread(datasetId, deadline)
            thread.start()
            thread
        } finally {
          lock.unlock()
        }
      }
      // wait until the helper thread has either acquired the lock or given up before retrying
      databaseWaiter.semaphore.acquire()
    }
    sys.error("Can't get here")
  }

  private class LockThread(datasetId: String, deadline: Long) extends Thread {
    val semaphore = new java.util.concurrent.Semaphore(0)
    override def run() {
      val lock =
        try { lockProvider.lock(lockNameFor(datasetId), math.max(1L, (deadline - System.nanoTime()) / 1000000L)) }
        catch { case e: LockTimeoutException => return }
        finally { semaphore.release() }
      try {
        for {
          conn <- managed(dataSource.getConnection())
          stmt <- managed(conn.prepareStatement("SELECT system_id FROM dataset_map WHERE dataset_id = ? FOR UPDATE"))
        } {
          stmt.setString(1, datasetId)
          stmt.executeQuery().close()
        }
      } finally {
        lock.unlock()
      }
    }
  }

  private class Retry extends scala.util.control.ControlThrowable
}
