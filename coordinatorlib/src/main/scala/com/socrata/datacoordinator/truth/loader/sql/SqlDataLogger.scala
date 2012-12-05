package com.socrata.datacoordinator
package truth.loader
package sql

import java.sql.{PreparedStatement, Connection}

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.util.Counter
import com.socrata.datacoordinator.truth.RowLogCodec

class SqlDataLogger[CT, CV](connection: Connection, sqlizer: DataSqlizer[CT, CV], rowCodecFactory: () => RowLogCodec[CV]) extends DataLogger[CV] {
  val log = org.slf4j.LoggerFactory.getLogger(getClass)

  var insertStmt: PreparedStatement = null
  var totalSize = 0
  var batched = 0
  var baos: java.io.ByteArrayOutputStream = _
  var out: java.io.DataOutputStream = _
  var rowCodec: RowLogCodec[CV] = _
  var didOne: Boolean = _
  reset()

  lazy val versionNum = for {
    stmt <- managed(connection.createStatement())
    rs <- managed(stmt.executeQuery("SELECT COALESCE(MAX(version), 0) FROM " + sqlizer.logTableName))
  } yield {
    val hasNext = rs.next()
    assert(hasNext, "next version query didn't return anything?")
    rs.getLong(1) + 1
  }

  val nextSubVersionNum = new Counter(init = 1)

  def reset() {
    baos = new java.io.ByteArrayOutputStream
    out = new java.io.DataOutputStream(new org.xerial.snappy.SnappyOutputStream(baos))
    didOne = false
    rowCodec = rowCodecFactory()
    rowCodec.writeVersion(out)
  }

  def maybeFlush() {
    didOne=true
    if(baos.size > 128000) {
      flushInner()
      if(totalSize > 2000000) flushOuter()
    }
  }

  def flushInner() {
    if(didOne) {
      out.close()
      val bytes = baos.toByteArray
      reset()
      batchRowLogLine(bytes)
    }
  }

  def batchRowLogLine(bytes: Array[Byte]) {
    if(insertStmt == null) {
      insertStmt = connection.prepareStatement("INSERT INTO " + sqlizer.logTableName + " (version, subversion, what, aux) VALUES (" + versionNum + ", ?, 'rows', ?)")
    }

    insertStmt.setLong(1, nextSubVersionNum())
    insertStmt.setBytes(2, bytes)
    insertStmt.addBatch()

    totalSize += bytes.length
    batched += 1
  }

  def flushOuter() {
    if(batched != 0) {
      log.debug("Flushing {} log rows", batched)

      batched = 0
      totalSize = 0

      insertStmt.executeBatch()
    }
  }

  def insert(sid: Long, row: Row[CV]) {
    rowCodec.insert(out, sid, row)
    maybeFlush()
  }

  def update(sid: Long, row: Row[CV]) {
    rowCodec.update(out, sid, row)
    maybeFlush()
  }

  def delete(systemID: Long) {
    rowCodec.delete(out, systemID)
    maybeFlush()
  }

  def finish() = {
    flushInner()
    flushOuter()
    versionNum
  }

  def close() {
    if(insertStmt != null) insertStmt.close()
  }
}
