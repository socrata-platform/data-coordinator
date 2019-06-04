package com.socrata.datacoordinator
package truth.loader
package sql

import java.sql.{PreparedStatement, Connection}

import com.socrata.datacoordinator.util.TimingReport
import com.socrata.datacoordinator.truth.RowLogCodec
import com.rojoma.simplearm.util._

class SqlLogger[CT, CV](connection: Connection,
                        auditTableName: String,
                        user: String,
                        logTableName: String,
                        rowCodecFactory: () => RowLogCodec[CV],
                        timingReport: TimingReport,
                        rowFlushSize: Int = 128000,
                        batchFlushSize: Int = 2000000)
  extends AbstractSqlLogger[CT, CV](connection, auditTableName, user, logTableName, rowCodecFactory, timingReport, rowFlushSize)
{
  import SqlLogger._

  private[this] var _insertStmt: PreparedStatement = null

  private[this] var totalSize = 0
  private[this] var batched = 0

  private def insertStmt = {
    if(_insertStmt == null) {
      writeAudit()
      _insertStmt = connection.prepareStatement("INSERT INTO " + logTableName + " (version, subversion, what, aux) VALUES (" + versionNum + ", ?, ?, ?)")
    }
    _insertStmt
  }

  protected def logLine(what: String, data: Array[Byte]) {
    doLog(what, data, nextSubVersionNum())
  }

  private def doLog(what: String, data: Array[Byte], subVersionNum: Long) {
    val i = insertStmt
    i.setLong(1, subVersionNum)
    i.setString(2, what)
    i.setBytes(3, data)
    i.addBatch()

    totalSize += data.length
    batched += 1

    if(totalSize > batchFlushSize) flushBatch()
  }

  protected def logRowsChangePreview(rowsChangedPreviewSubversion: Long, what: String, data: Array[Byte]) {
    doLog(what, data, rowsChangedPreviewSubversion)
  }

  protected def flushBatch() {
    if(batched != 0) {
      timingReport("flush-log-batch", "count" -> batched) {
        log.debug("Flushing {} log rows", batched)

        batched = 0
        totalSize = 0

        insertStmt.executeBatch()
      }
    }
  }

  def close() {
    if(_insertStmt != null) {
      _insertStmt.close()
      _insertStmt = null
    }
  }
}

object SqlLogger {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[SqlLogger[_,_]])

  val maxOpLength = 8

  // all of these must be at most 8 characters long and consist of
  // nothing but lower-case ASCII letters.
  val RowDataUpdated = "rowdata"
  val CounterUpdated = "cnterup"

  val Truncated = "truncate"
  val ColumnCreated = "colcreat"
  val ColumnRemoved = "coldel"
  val ComputationStrategyCreated = "strat"
  val ComputationStrategyRemoved = "unstrat"
  val FieldNameUpdated = "fieldnam"
  val RowIdentifierSet = "ridcol"
  val RowIdentifierCleared = "noridcol"
  val SystemRowIdentifierChanged = "sidcol"
  val VersionColumnChanged = "vercol"
  val LastModifiedChanged = "lm"
  val WorkingCopyCreated = "workcopy"
  val DataCopied = "datacopy"
  val WorkingCopyDropped = "dropwork"
  val SnapshotDropped = "dropsnap"
  val WorkingCopyPublished = "pubwork"
  val RollupCreatedOrUpdated = "rollupcr"
  val RollupDropped = "rollupdr"
  val TransactionEnded = "endtxn"
  val RowsChangedPreview = "rowcount"
  val SecondaryReindex = "secreidx"
  val SecondaryAddIndex = "secadidx"

  val allEvents = for {
    method <- getClass.getDeclaredMethods
    if java.lang.reflect.Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0
    if method.getReturnType == classOf[String]
  } yield method.invoke(this).asInstanceOf[String]
}
