package com.socrata.datacoordinator
package truth.loader
package sql

import scala.io.Codec

import java.sql.{PreparedStatement, Connection}

import com.rojoma.json.ast._
import com.rojoma.json.io.CompactJsonWriter
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.util.Counter
import com.socrata.datacoordinator.truth.RowLogCodec

class SqlLogger[CT, CV](connection: Connection,
                        sqlizer: DataSqlizer[CT, CV],
                        rowCodecFactory: () => RowLogCodec[CV],
                        rowFlushSize: Int = 128000,
                        batchFlushSize: Int = 2000000)
  extends Logger[CT, CV]
{
  import SqlLogger.log

  lazy val versionNum = for {
    stmt <- managed(connection.createStatement())
    rs <- managed(stmt.executeQuery("SELECT MAX(version) FROM " + sqlizer.logTableName))
  } yield {
    val hasNext = rs.next()
    assert(hasNext, "next version query didn't return anything?")
    // MAX(version) will be null if there is no data in the log table;
    // ResultSet#getLong returns 0 if the value was null.
    rs.getLong(1) + 1
  }

  val nullBytes = Codec.toUTF8("null")
  var transactionEnded = false
  val nextSubVersionNum = new Counter(init = 1)

  var _insertStmt: PreparedStatement = null

  var totalSize = 0
  var batched = 0

  def insertStmt = {
    if(_insertStmt == null) {
      _insertStmt = connection.prepareStatement("INSERT INTO " + sqlizer.logTableName + " (version, subversion, what, aux) VALUES (" + versionNum + ", ?, ?, ?)")
    }
    _insertStmt
  }

  def logLine(what: String, data: Array[Byte]) {
    val i = insertStmt
    i.setLong(1, nextSubVersionNum())
    i.setString(2, what)
    i.setBytes(3, data)
    i.addBatch()

    totalSize += data.length
    batched += 1
  }

  def maybeFlushBatch() {
    if(totalSize > batchFlushSize) flushBatch()
  }

  def flushBatch() {
    if(batched != 0) {
      log.debug("Flushing {} log rows", batched)

      batched = 0
      totalSize = 0

      insertStmt.executeBatch()
    }
  }

  def checkTxn() {
    assert(!transactionEnded, "Operation logged after saying the transaction was over")
  }

  def columnCreated(name: String, typ: CT) {
    checkTxn()
    flushRowData()
    logLine(SqlLogger.ColumnCreated, Codec.toUTF8(CompactJsonWriter.toString(JObject(Map(
      "c" -> JString(name),
      "t" -> JString(sqlizer.typeContext.nameFromType(typ))
    )))))
  }

  def columnRemoved(name: String) {
    checkTxn()
    flushRowData()
    logLine(SqlLogger.ColumnRemoved, Codec.toUTF8(CompactJsonWriter.toString(JObject(Map("c" -> JString(name))))))
  }

  def rowIdentifierChanged(name: Option[String]) {
    checkTxn()
    flushRowData()
    val nameJson = name match {
      case Some(n) => JString(n)
      case None => JNull
    }
    logLine(SqlLogger.RowIdentifierChanged, Codec.toUTF8(CompactJsonWriter.toString(JObject(Map("c" -> nameJson)))))
  }

  def workingCopyCreated() {
    checkTxn()
    flushRowData()

    logLine(SqlLogger.WorkingCopyCreated, nullBytes)
  }

  def workingCopyDropped() {
    checkTxn()
    flushRowData()
    logLine(SqlLogger.WorkingCopyDropped, nullBytes)
  }

  def workingCopyPublished() {
    checkTxn()
    flushRowData()
    logLine(SqlLogger.WorkingCopyPublished, nullBytes)
  }

  def endTransaction() {
    checkTxn()
    transactionEnded = true

    flushRowData()

    if(nextSubVersionNum.peek != 1) logLine(SqlLogger.TransactionEnded, nullBytes)
    flushBatch()
  }

  // DataLogger facet starts here

  var baos: java.io.ByteArrayOutputStream = _
  var out: java.io.DataOutputStream = _
  var rowCodec: RowLogCodec[CV] = _
  var didOne: Boolean = _
  reset()

  def reset() {
    baos = new java.io.ByteArrayOutputStream
    out = new java.io.DataOutputStream(new org.xerial.snappy.SnappyOutputStream(baos))
    didOne = false
    rowCodec = rowCodecFactory()
    rowCodec.writeVersion(out)
  }

  def maybeFlushRowData() {
    didOne=true
    if(baos.size > rowFlushSize) {
      flushRowData()
    }
  }

  def flushRowData() {
    flushInner()
    maybeFlushBatch()
  }

  def flushInner() {
    if(didOne) {
      out.close()
      val bytes = baos.toByteArray
      reset()
      logLine(SqlLogger.RowDataUpdated, bytes)
    }
  }

  def insert(sid: Long, row: Row[CV]) {
    checkTxn()
    rowCodec.insert(out, sid, row)
    maybeFlushRowData()
  }

  def update(sid: Long, row: Row[CV]) {
    checkTxn()
    rowCodec.update(out, sid, row)
    maybeFlushRowData()
  }

  def delete(systemID: Long) {
    checkTxn()
    rowCodec.delete(out, systemID)
    maybeFlushRowData()
  }

  def close() {
    if(_insertStmt != null) _insertStmt.close()
  }
}

object SqlLogger {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[SqlLogger[_,_]])

  // all of these must be exactly 3 characters long and consist of
  // nothing but upper-case ASCII letters.
  val RowDataUpdated = "ROW"
  val DatasetTruncated = "TRN"
  val ColumnCreated = "CCR"
  val ColumnRemoved = "CRM"
  val RowIdentifierChanged = "RID"
  val WorkingCopyCreated = "CWC"
  val WorkingCopyDropped = "DWC"
  val WorkingCopyPublished = "PUB"
  val TransactionEnded = "END"

  private def good(s: String) = s.length == 3 && s.forall { c => c >= 'A' && c <= 'Z' }

  for {
    method <- getClass.getDeclaredMethods
    if java.lang.reflect.Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0
    if method.getReturnType == classOf[String]
  } assert(good(method.invoke(this).asInstanceOf[String]), method.getName + " is either not 3 characters long or contains something which isn't an uppercase letter")
}
