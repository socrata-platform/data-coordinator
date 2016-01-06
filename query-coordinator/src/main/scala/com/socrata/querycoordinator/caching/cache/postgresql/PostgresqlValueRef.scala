package com.socrata.querycoordinator.caching.cache.postgresql

import java.io.{ByteArrayInputStream, IOException, InputStream}
import java.sql.Connection

import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.querycoordinator.caching.cache.ValueRef
import com.socrata.util.io.StreamWrapper

class PostgresqlValueRef(connection: Connection, key: Long, scope: ResourceScope, streamWrapper: StreamWrapper) extends ValueRef {
  private var closed = false

  override def open(scope: ResourceScope): InputStream = synchronized {
    if(closed) throw new IOException("Closed ValueRef")
    val stmt = scope.open(connection.prepareStatement("SELECT data FROM cache_data WHERE cache_id = ? ORDER BY chunknum"))
    stmt.setLong(1, key)
    stmt.setFetchSize(1)
    val rs = scope.open(stmt.executeQuery(), transitiveClose = List(stmt))
    val rawStream = scope.open(new InputStream {
      var current: ByteArrayInputStream = null
      var seenEOF = false

      def read(): Int =
        if(ensureAvailable()) current.read()
        else -1

      override def read(bs: Array[Byte], off: Int, len: Int): Int =
        if(ensureAvailable()) current.read(bs, off, len)
        else -1

      def ensureAvailable(): Boolean = {
        while(current == null || current.available == 0) {
          if(seenEOF) return false
          if(!rs.next()) { seenEOF = true; return false }
          current = new ByteArrayInputStream(rs.getBytes(1))
        }
        true
      }
    }, transitiveClose = List(rs))
    streamWrapper.wrapInputStream(rawStream, scope)
  }

  override def close(): Unit = synchronized {
    if(!closed) {
      closed = true
    }
  }
}
