package com.socrata.querycoordinator.caching.cache.postgresql

import java.io.OutputStream
import java.sql.{Connection, Timestamp}
import javax.sql.DataSource

import com.rojoma.simplearm.v2._
import com.socrata.querycoordinator.caching.cache.{ValueRef, CacheSession}
import com.socrata.querycoordinator.caching.{CloseBlockingOutputStream, ChunkingOutputStream}
import com.socrata.util.io.StreamWrapper
import org.postgresql.util.PSQLException

import scala.concurrent.duration.FiniteDuration

class PostgresqlCacheSession(ds: DataSource, updateATimeInterval: FiniteDuration, streamWrapper: StreamWrapper) extends CacheSession {
  private val updateATimeMS = updateATimeInterval.toMillis
  private val scope = new ResourceScope
  private var closed = false
  @volatile private var readConnection: Connection = null
  @volatile private var writeConnection: Connection = null
  private var updateOnClose = Set.empty[Long]

  private def initRead(): Unit = {
    if(closed) throw new IllegalStateException("Accessing a closed session")
    if(readConnection eq null) {
      readConnection = scope.open(ds.getConnection)
      readConnection.setReadOnly(true)
      readConnection.setAutoCommit(true)
    }
  }

  private def initWrite(): Unit = {
    if(closed) throw new IllegalStateException("Accessing a closed session")
    if(writeConnection eq null) {
      writeConnection = scope.open(ds.getConnection)
      writeConnection.setReadOnly(false)
      writeConnection.setAutoCommit(false)
    }
  }

  override def find(key: String, resourceScope: ResourceScope): Option[ValueRef] = synchronized {
    initRead()
    using(readConnection.prepareStatement("select id, approx_last_access from cache where key = ?")) { stmt =>
      stmt.setString(1, key)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          val dataKey = rs.getLong(1)
          val approx_last_access = rs.getTimestamp(2)

          if(approx_last_access.getTime < System.currentTimeMillis - updateATimeMS) updateOnClose += dataKey

          Some(resourceScope.open(new PostgresqlValueRef(readConnection, dataKey, scope, streamWrapper)))
        } else {
          None
        }
      }
    }
  }

  override def create(key: String)(filler: OutputStream => Unit): Unit = synchronized {
    initWrite()

    try {
      val id = using(writeConnection.prepareStatement("INSERT INTO cache (key, created_at, approx_last_access) VALUES (NULL, ?, ?) RETURNING id")) { stmt =>
        val now = new Timestamp(System.currentTimeMillis())
        stmt.setTimestamp(1, now)
        stmt.setTimestamp(2, now)
        using(stmt.executeQuery()) { rs =>
          if(!rs.next()) sys.error("insert..returning did not return anything?")
          rs.getLong(1)
        }
      }

      using(writeConnection.prepareStatement("INSERT INTO cache_data (cache_id, chunknum, data) VALUES (?, ?, ?)")) { stmt =>
        val os = new ChunkingOutputStream(65536) {
          var chunkNum = 0
          def onChunk(bytes: Array[Byte]): Unit = {
            stmt.setLong(1, id)
            stmt.setInt(2, chunkNum); chunkNum += 1
            stmt.setBytes(3, bytes)
            stmt.execute()
          }
        }
        using(new ResourceScope) { rs =>
          val stream = streamWrapper.wrapOutputStream(rs.open(new CloseBlockingOutputStream(os)), rs)
          filler(stream)
        }
        os.close()
      }

      writeConnection.commit() // ok, we have created (but not linked in) our entry

      def createLink(): Boolean = {
        val oldIsolationLevel = writeConnection.getTransactionIsolation
        try {
          writeConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)

          val oldRow =
            using(writeConnection.prepareStatement("SELECT id FROM cache WHERE key = ? FOR UPDATE")) { stmt =>
              stmt.setString(1, key)
              using(stmt.executeQuery()) { rs =>
                if(rs.next()) {
                  Some(rs.getLong(1))
                } else {
                  None
                }
              }
            }

          oldRow.foreach { oldId =>
            using(writeConnection.prepareStatement("UPDATE cache SET key = null WHERE id = ?")) { stmt =>
              stmt.setLong(1, oldId)
              stmt.execute()
            }

            using(writeConnection.prepareStatement("INSERT INTO pending_deletes (cache_id, delete_queued) VALUES (?, ?)")) { stmt =>
              stmt.setLong(1, oldId)
              stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()))
              stmt.execute()
            }
            oldId
          }

          using(writeConnection.prepareStatement("UPDATE cache SET key = ? WHERE id = ?")) { stmt =>
            stmt.setString(1, key)
            stmt.setLong(2, id)
            stmt.execute()
          }

          writeConnection.commit()
          true
        } catch {
          case e: PSQLException if e.getSQLState == "40001" /* serialization_failure */ =>
            false
        } finally {
          writeConnection.rollback()
          writeConnection.setTransactionIsolation(oldIsolationLevel)
        }
      }

      while(!createLink()) {}
    } finally {
      writeConnection.rollback()
    }
  }

  def close(): Unit = synchronized {
    if(!closed) {
      closed = true
      scope.close()
      if(updateOnClose.nonEmpty) {
        using(ds.getConnection()) { conn =>
          conn.setAutoCommit(true)
          conn.setReadOnly(false)
          using(conn.createStatement()) { stmt =>
            stmt.execute(updateOnClose.mkString("UPDATE cache SET approx_last_access = NOW() WHERE id IN (",",",")"))
          }
        }
      }
    }
  }

  def closeAbnormally(): Unit = synchronized {
    if(!closed) {
      closed = true
      scope.close()
    }
  }
}
