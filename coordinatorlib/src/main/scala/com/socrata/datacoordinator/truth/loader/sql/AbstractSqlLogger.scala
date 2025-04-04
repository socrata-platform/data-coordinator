package com.socrata.datacoordinator
package truth.loader
package sql

import java.sql.Connection
import com.socrata.datacoordinator.truth.RowLogCodec

import java.util.zip.{Deflater, DeflaterOutputStream}
import com.rojoma.json.v3.ast.{JBoolean, JObject}
import com.socrata.datacoordinator.util.{Counter, TimingReport}
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, ComputationStrategyInfo, CopyInfo, IndexInfo, RollupInfo}
import com.socrata.datacoordinator.id.{IndexName, RowId}
import com.rojoma.simplearm.v2._
import com.socrata.soql.environment.ColumnName
import org.joda.time.DateTime
import messages.LogData

abstract class AbstractSqlLogger[CT, CV](val connection: Connection,
                                         val auditTableName: String,
                                         val user: String,
                                         val logTableName: String,
                                         rowCodecFactory: () => RowLogCodec[CV],
                                         val timingReport: TimingReport,
                                         rowFlushSize: Int = 128000)
  extends Logger[CT, CV]
{
  import SqlLogger._
  import messages.ToProtobuf._

  protected def logLine(what: String, aux: Array[Byte])
  protected def logRowsChangePreview(subVersion: Long, what: String, aux: Array[Byte])
  protected def flushBatch()

  protected lazy val versionNum = timingReport("version-num", "audit-table" -> auditTableName) {
    for {
      stmt <- managed(connection.createStatement())
      rs <- managed(stmt.executeQuery("SELECT MAX(version) FROM " + auditTableName))
    } {
      val hasNext = rs.next()
      assert(hasNext, "next version query didn't return anything?")
      // MAX(version) will be null if there is no data in the log table;
      // ResultSet#getLong returns 0 if the value was null.
      rs.getLong(1) + 1
    }
  }

  private[this] var transactionEnded = false
  protected[this] val nextSubVersionNum = new Counter(init = 1)

  private[this] def checkTxn() {
    assert(!transactionEnded, "Operation logged after saying the transaction was over")
  }

  protected[this] def writeAudit() {
    using(connection.prepareStatement("INSERT INTO " + auditTableName + " (version, who) VALUES (?, ?)")) { stmt =>
      stmt.setLong(1, versionNum)
      stmt.setString(2, user)
      stmt.executeUpdate()
    }
  }

  private def logLine[T](what: String, aux: scalapb.GeneratedMessage) {
    logLine(what, aux.toByteArray)
  }

  def columnCreated(info: ColumnInfo[CT]) {
    checkTxn()
    flushRowData()
    logLine(ColumnCreated, LogData.ColumnCreated(convert(info.unanchored)))
  }

  def columnRemoved(info: ColumnInfo[CT]) {
    checkTxn()
    flushRowData()
    logLine(ColumnRemoved, LogData.ColumnRemoved(convert(info.unanchored)))
  }

  def computationStrategyCreated(info: ColumnInfo[CT], cs: ComputationStrategyInfo): Unit = {
    checkTxn()
    flushRowData()
    logLine(ComputationStrategyCreated, LogData.ComputationStrategyCreated(convert(info.unanchored)))
  }

  def computationStrategyRemoved(info: ColumnInfo[CT]): Unit = {
    checkTxn()
    flushRowData()
    logLine(ComputationStrategyRemoved, LogData.ComputationStrategyRemoved(convert(info.unanchored)))
  }

  def fieldNameUpdated(info: ColumnInfo[CT]): Unit = {
    checkTxn()
    flushRowData()
    assert(info.fieldName.isDefined, "Got a field name updated without a field name?  This should be impossible.")
    logLine(FieldNameUpdated, LogData.FieldNameUpdated(convert(info.unanchored)))
  }

  def rowIdentifierSet(info: ColumnInfo[CT]) {
    checkTxn()
    flushRowData()
    logLine(RowIdentifierSet, LogData.RowIdentifierSet(convert(info.unanchored)))
  }

  def rowIdentifierCleared(info: ColumnInfo[CT]) {
    checkTxn()
    flushRowData()
    logLine(RowIdentifierCleared, LogData.RowIdentifierCleared(convert(info.unanchored)))
  }

  def systemIdColumnSet(info: ColumnInfo[CT]) {
    checkTxn()
    flushRowData()
    logLine(SystemRowIdentifierChanged, LogData.SystemIdColumnSet(convert(info.unanchored)))
  }

  def versionColumnSet(info: ColumnInfo[CT]) {
    checkTxn()
    flushRowData()
    logLine(VersionColumnChanged, LogData.VersionColumnSet(convert(info.unanchored)))
  }

  def lastModifiedChanged(lastModified: DateTime) {
    checkTxn()
    flushRowData()
    logLine(LastModifiedChanged, LogData.LastModifiedChanged(convert(lastModified)))
  }

  def workingCopyCreated(info: CopyInfo) {
    checkTxn()
    flushRowData()
    logLine(WorkingCopyCreated, LogData.WorkingCopyCreated(
      convert(info.datasetInfo.unanchored),
      convert(info.unanchored))
    )
  }

  def dataCopied() {
    checkTxn()
    flushRowData()

    logLine(DataCopied, LogData.DataCopied.defaultInstance)
  }

  def snapshotDropped(info: CopyInfo) {
    checkTxn()
    flushRowData()
    logLine(SnapshotDropped, LogData.SnapshotDropped(convert(info.unanchored)))
  }

  def workingCopyDropped() {
    checkTxn()
    flushRowData()
    logLine(WorkingCopyDropped, LogData.WorkingCopyDropped.defaultInstance)
  }

  def workingCopyPublished() {
    checkTxn()
    flushRowData()
    logLine(WorkingCopyPublished, LogData.WorkingCopyPublished.defaultInstance)
  }

  def rollupCreatedOrUpdated(info: RollupInfo) {
    checkTxn()
    flushRowData()
    logLine(RollupCreatedOrUpdated, LogData.RollupCreatedOrUpdated(convert(info.unanchored)))
  }

  def rollupDropped(info: RollupInfo) {
    checkTxn()
    flushRowData()
    logLine(RollupDropped, LogData.RollupDropped(convert(info.unanchored)))
  }

  def secondaryReindex() = {
    checkTxn()
    logLine(SecondaryReindex, LogData.SecondaryReindex.defaultInstance)
  }

  def indexDirectiveCreatedOrUpdated(info: ColumnInfo[CT], directive: JObject) = {
    checkTxn()
    val enabled = directive.get("enabled") match {
      case Some(JBoolean(b)) => b
      case _ => false
    }
    val search = directive.get("search") match {
      case Some(JBoolean(b)) => Some(b)
      case _ => Some(true)
    }
    logLine(IndexDirectiveCreatedOrUpdated, LogData.IndexDirectiveCreatedOrUpdated(convert(info.unanchored), enabled, search))
  }

  def indexDirectiveDropped(info: ColumnInfo[CT]) = {
    checkTxn()
    logLine(IndexDirectiveDropped, LogData.IndexDirectiveDropped(convert(info.unanchored)))
  }

  def indexCreatedOrUpdated(info: IndexInfo): Unit = {
    checkTxn()
    flushRowData()
    logLine(IndexCreatedOrUpdated, LogData.IndexCreatedOrUpdated(convert(info.unanchored)))
  }

  def indexDropped(name: IndexName): Unit = {
    checkTxn()
    flushRowData()
    logLine(IndexDropped, LogData.IndexDropped(name.underlying))
  }

  def endTransaction() = {
    checkTxn()
    transactionEnded = true

    flushRowData()

    if(nextSubVersionNum.peek != nextSubVersionNum.init) {
      logLine(TransactionEnded, LogData.EndTransaction.defaultInstance)
      flushBatch()
      Some(versionNum)
    } else {
      None
    }
  }

  def counterUpdated(nextCounter: Long) {
    checkTxn()
    flushRowData()
    logLine(CounterUpdated, LogData.CounterUpdated(nextCounter))
  }

  // DataLogger facet starts here

  private[this] var rowsInserted: Long = 0
  private[this] var rowsUpdated: Long = 0
  private[this] var rowsDeleted: Long = 0
  private[this] var baos: java.io.ByteArrayOutputStream = _
  private[this] var underlyingOutputStream: java.io.OutputStream = _
  private[this] var out: com.google.protobuf.CodedOutputStream = _
  private[this] var rowCodec: RowLogCodec[CV] = _
  private[this] var dataStart: Option[Long] = None
  private[this] var dataTruncated = false

  private sealed abstract class RowDataState
  private case object NotDoingRowData extends RowDataState
  private case object WroteTruncated extends RowDataState
  private case object WroteRows extends RowDataState

  private[this] var rowDataState: RowDataState = NotDoingRowData
  resetRowBuffer()

  private def resetRowBuffer() {
    baos = new java.io.ByteArrayOutputStream

    /*
    baos.write(0) // "we're using Snappy"
    underlyingOutputStream = new org.xerial.snappy.SnappyOutputStream(baos)
    */

    /*
    baos.write(1) // "no compression"
    underlyingOutputStream = baos
    */

    /*
    baos.write(2) // "deflate"
    underlyingOutputStream = new DeflaterOutputStream(baos)
    */

    baos.write(3) // "pure java snappy"
    underlyingOutputStream = new snappy.SnappyOutputStream(baos)

    /*
    baos.write(4) // "pure java snappy with framing"
    underlyingOutputStream = new snappy.SnappyFramedOutputStream(baos)
    */

    out = com.google.protobuf.CodedOutputStream.newInstance(underlyingOutputStream)

    rowCodec = rowCodecFactory()
    rowCodec.writeVersion(out)
  }

  private def maybeFlushRowData() {
    rowDataState=WroteRows
    if(baos.size > rowFlushSize) {
      flushRowData(atEnd = false)
    }
  }

  private def noteDataStart(): Unit = {
    dataStart match {
      case None =>
        dataStart = Some(nextSubVersionNum()) // reserve a slot for the row preview
      case Some(_) =>
        // we're already startd
    }
  }

  private def noteDataEnd(): Unit = {
    dataStart.foreach { subVersion =>
      val aux = LogData.RowsChangedPreview(rowsInserted, rowsUpdated, rowsDeleted, dataTruncated)
      logRowsChangePreview(subVersion, RowsChangedPreview, aux.toByteArray)
    }
    dataStart = None
    dataTruncated = false
  }

  private def flushRowData(atEnd: Boolean = true) {
    if(rowDataState != NotDoingRowData) {
      if(rowDataState == WroteRows) {
        out.flush()
        underlyingOutputStream.close()
        val bytes = baos.toByteArray

        resetRowBuffer()
        logLine(RowDataUpdated, bytes)
      }

      if(atEnd) {
        noteDataEnd()
        rowDataState = NotDoingRowData
      }
    }
  }

  def truncated() {
    checkTxn()
    flushRowData()
    noteDataStart()
    logLine(Truncated, LogData.Truncated())
    dataTruncated = true
    rowDataState = WroteTruncated
  }

  def insert(sid: RowId, row: Row[CV]) {
    checkTxn()
    noteDataStart()
    rowsInserted += 1
    rowCodec.insert(out, sid, row)
    maybeFlushRowData()
  }

  def update(sid: RowId, oldRow: Option[Row[CV]], newRow: Row[CV]) {
    checkTxn()
    noteDataStart()
    rowsUpdated += 1
    rowCodec.update(out, sid, oldRow, newRow)
    maybeFlushRowData()
  }

  def delete(systemID: RowId, oldRow: Option[Row[CV]]) {
    checkTxn()
    noteDataStart()
    rowsDeleted += 1
    rowCodec.delete(out, systemID, oldRow)
    maybeFlushRowData()
  }
}
