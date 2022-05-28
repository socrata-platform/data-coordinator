package com.socrata.datacoordinator
package truth.loader.sql

import scala.io.Codec
import java.sql.{Connection, PreparedStatement, ResultSet}
import com.rojoma.json.v3.ast.{JBoolean, JObject}
import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.id.IndexName
import com.socrata.datacoordinator.truth.RowLogCodec
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.util.{CloseableIterator, LeakDetect}
import com.socrata.soql.environment.ColumnName
import messages.LogData

class SqlDelogger[CV](connection: Connection,
                      logTableName: String,
                      rowCodecFactory: () => RowLogCodec[CV])
  extends Delogger[CV]
{
  import messages.FromProtobuf._

  require(!connection.getAutoCommit, "connection is in auto-commit mode") // need non-AC in order to do streaming reads

  private val rs = new ResourceScope
  var _queryStmt: PreparedStatement = null
  var _queryTypesStmt: PreparedStatement = null

  def queryStmt = {
    if(_queryStmt == null) {
      _queryStmt = rs.open(connection.prepareStatement("select subversion, what, aux from " + logTableName + " where version = ? order by subversion"))
      _queryStmt.setFetchSize(1)
    }
    _queryStmt
  }

  def queryTypesStmt = {
    if(_queryTypesStmt == null) {
      _queryTypesStmt = rs.open(connection.prepareStatement("select subversion, what from " + logTableName + " where version = ? order by subversion"))
      _queryTypesStmt.setFetchSize(1000)
    }
    _queryTypesStmt
  }

  def findPublishEvent(fromVersion: Long, toVersion: Long): Option[Long] =
    using(connection.prepareStatement("select version from " + logTableName + " where version >= ? and version <= ? and subversion = 1 and what = ? order by version limit 1")) { stmt =>
      stmt.setLong(1, fromVersion)
      stmt.setLong(2, toVersion)
      stmt.setString(3, SqlLogger.WorkingCopyPublished)
      using(stmt.executeQuery())(versionFromResultSet)
    }

  def versionFromResultSet(rs: ResultSet) =
    if(rs.next()) Some(rs.getLong("version"))
    else None

  def lastWorkingCopyCreatedVersion: Option[Long] =
    using(connection.prepareStatement("select version from " + logTableName + " where subversion = 1 and what = ? order by version desc limit 1")) { stmt =>
      stmt.setString(1, SqlLogger.WorkingCopyCreated)
      using(stmt.executeQuery())(versionFromResultSet)
    }

  def lastWorkingCopyDroppedOrPublishedVersion: Option[Long] =
    using(connection.prepareStatement("select version from " + logTableName + " where subversion = 1 and what = ? or what = ? order by version desc limit 1")) { stmt =>
      stmt.setString(1, SqlLogger.WorkingCopyPublished)
      stmt.setString(2, SqlLogger.WorkingCopyDropped)
      using(stmt.executeQuery())(versionFromResultSet)
    }

  def lastVersion: Option[Long] =
    using(connection.prepareStatement("select max(version) AS version from " + logTableName)) { stmt =>
      using(stmt.executeQuery())(versionFromResultSet)
    }

  def delog(version: Long) = {
    val it = new LogIterator(version) with LeakDetect
    try {
      it.advance()
    } catch {
      case _: NoEndOfTransactionMarker =>
        it.close()
        throw new MissingVersion(version, "Version " + version + " not found in log " + logTableName)
    }
    it
  }

  def delogOnlyTypes(version: Long) = {
    val it = new LogTypesIterator(version) with LeakDetect
    try {
      it.advance()
    } catch {
      case _: NoEndOfTransactionMarker =>
        it.close()
        throw new MissingVersion(version, "Version " + version + " not found in log " + logTableName)
    }
    it
  }

  abstract class AbstractLogIterator[T](iteratorType: String, stmt: PreparedStatement, version: Long) extends CloseableIterator[T] {
    assert(stmt != null)
    var rs: ResultSet = null
    var lastSubversion = 0L
    var nextResult: Option[T] = None
    var done = false
    val UTF8 = Codec.UTF8.charSet

    def errMsg(m: String) = m + " in version " + version + " of log " + logTableName

    override def toString() = {
      // this exists because otherwise Iterator#toString calls hasNext.
      // Since this hasNext has side-effects, it causes unpredictable
      // behaviour when tracing through it and IDEA tries to call
      // toString on "this".
      val state = if(!done) "unfinished" else "finished"
      s"$iteratorType($state)"
    }

    def advance() {
      nextResult = None
      if(rs == null) {
        stmt.setLong(1, version)
        rs = stmt.executeQuery()
      }
      decodeNext()
      done = nextResult == None
    }

    def hasNext = {
      if(done) false
      else {
        if(nextResult == None) advance()
        !done
      }
    }

    def next() =
      if(hasNext) {
        val r = nextResult.get
        advance()
        r
      } else {
        Iterator.empty.next()
      }

    def close() {
      if(rs != null) {
        rs.close()
        rs = null
      }
    }

    def decodeNext() {
      if(!rs.next()) throw new NoEndOfTransactionMarker(version, errMsg("No end of transaction"))
      val subversion = rs.getLong("subversion")
      if(subversion != lastSubversion + 1)
        throw new SkippedSubversion(version, lastSubversion + 1, subversion,
          errMsg(s"subversion skipped?  Got $subversion expected ${lastSubversion + 1}"))
      lastSubversion = subversion

      val op = rs.getString("what")

      nextResult = decode(op, rs)
    }

    def decode(op: String, rs: ResultSet): Option[T]
  }

  class LogTypesIterator(version: Long) extends AbstractLogIterator[Delogger.LogEventCompanion]("LogTypesIterator", queryTypesStmt, version) {
    def decode(op: String, rs: ResultSet) =
      op match {
        case SqlLogger.RowDataUpdated => Some(Delogger.RowDataUpdated)
        case SqlLogger.CounterUpdated => Some(Delogger.CounterUpdated)
        case SqlLogger.ColumnCreated => Some(Delogger.ColumnCreated)
        case SqlLogger.ColumnRemoved => Some(Delogger.ColumnRemoved)
        case SqlLogger.ComputationStrategyCreated => Some(Delogger.ComputationStrategyCreated)
        case SqlLogger.ComputationStrategyRemoved => Some(Delogger.ComputationStrategyRemoved)
        case SqlLogger.FieldNameUpdated => Some(Delogger.FieldNameUpdated)
        case SqlLogger.RowIdentifierSet => Some(Delogger.RowIdentifierSet)
        case SqlLogger.RowIdentifierCleared => Some(Delogger.RowIdentifierCleared)
        case SqlLogger.SystemRowIdentifierChanged => Some(Delogger.SystemRowIdentifierChanged)
        case SqlLogger.VersionColumnChanged => Some(Delogger.VersionColumnChanged)
        case SqlLogger.LastModifiedChanged => Some(Delogger.LastModifiedChanged)
        case SqlLogger.WorkingCopyCreated => Some(Delogger.WorkingCopyCreated)
        case SqlLogger.DataCopied => Some(Delogger.DataCopied)
        case SqlLogger.WorkingCopyDropped => Some(Delogger.WorkingCopyDropped)
        case SqlLogger.SnapshotDropped => Some(Delogger.SnapshotDropped)
        case SqlLogger.WorkingCopyPublished => Some(Delogger.WorkingCopyPublished)
        case SqlLogger.Truncated => Some(Delogger.Truncated)
        case SqlLogger.RollupCreatedOrUpdated => Some(Delogger.RollupCreatedOrUpdated)
        case SqlLogger.RollupDropped => Some(Delogger.RollupDropped)
        case SqlLogger.RowsChangedPreview => Some(Delogger.RowsChangedPreview)
        case SqlLogger.TransactionEnded =>
          assert(!rs.next(), "there was data after TransactionEnded?")
          None
        case SqlLogger.SecondaryReindex => Some(Delogger.SecondaryReindex)
        case SqlLogger.IndexDirectiveCreatedOrUpdated => Some(Delogger.IndexDirectiveCreatedOrUpdated)
        case SqlLogger.IndexDirectiveDropped => Some(Delogger.IndexDirectiveDropped)
        case SqlLogger.IndexCreatedOrUpdated => Some(Delogger.IndexCreatedOrUpdated)
        case SqlLogger.IndexDropped => Some(Delogger.IndexDropped)
        case other =>
          throw new UnknownEvent(version, op, errMsg(s"Unknown event $op"))
      }
  }

  class LogIterator(version: Long) extends AbstractLogIterator[Delogger.LogEvent[CV]]("LogIterator", queryStmt, version) {
    override def decode(op: String, rs: ResultSet) = {
      val aux = rs.getBytes("aux")
      op match {
        case SqlLogger.RowDataUpdated =>
          Some(decodeRowDataUpdated(aux))
        case SqlLogger.CounterUpdated =>
          Some(decodeCounterUpdated(aux))
        case SqlLogger.ColumnCreated =>
          Some(decodeColumnCreated(aux))
        case SqlLogger.ColumnRemoved =>
          Some(decodeColumnRemoved(aux))
        case SqlLogger.ComputationStrategyCreated =>
          Some(decodeComputationStrategyCreated(aux))
        case SqlLogger.ComputationStrategyRemoved =>
          Some(decodeComputationStrategyRemoved(aux))
        case SqlLogger.FieldNameUpdated =>
          Some(decodeFieldNameUpdated(aux))
        case SqlLogger.RowIdentifierSet =>
          Some(decodeRowIdentifierSet(aux))
        case SqlLogger.RowIdentifierCleared =>
          Some(decodeRowIdentifierCleared(aux))
        case SqlLogger.SystemRowIdentifierChanged =>
          Some(decodeSystemRowIdentifierChanged(aux))
        case SqlLogger.VersionColumnChanged =>
          Some(decodeVersionColumnChanged(aux))
        case SqlLogger.LastModifiedChanged =>
          Some(decodeLastModifiedChanged(aux))
        case SqlLogger.WorkingCopyCreated =>
          Some(decodeWorkingCopyCreated(aux))
        case SqlLogger.DataCopied =>
          Some(Delogger.DataCopied)
        case SqlLogger.WorkingCopyDropped =>
          Some(Delogger.WorkingCopyDropped)
        case SqlLogger.SnapshotDropped =>
          Some(decodeSnapshotDropped(aux))
        case SqlLogger.WorkingCopyPublished =>
          Some(Delogger.WorkingCopyPublished)
        case SqlLogger.Truncated =>
          Some(Delogger.Truncated)
        case SqlLogger.RollupCreatedOrUpdated =>
          Some(decodeRollupCreatedOrUpdated(aux))
        case SqlLogger.RollupDropped =>
          Some(decodeRollupDropped(aux))
        case SqlLogger.RowsChangedPreview =>
          Some(decodeRowsChangedPreview(aux))
        case SqlLogger.TransactionEnded =>
          assert(!rs.next(), "there was data after TransactionEnded?")
          None
        case SqlLogger.SecondaryReindex =>
          Some(Delogger.SecondaryReindex)
        case SqlLogger.IndexDirectiveCreatedOrUpdated =>
          Some(decodeIndexDirectiveCreatedOrUpdated(aux))
        case SqlLogger.IndexDirectiveDropped =>
          Some(decodeIndexDirectiveDropped(aux))
        case SqlLogger.IndexCreatedOrUpdated =>
          Some(decodeIndexCreatedOrUpdated(aux))
        case SqlLogger.IndexDropped =>
          Some(decodeIndexDropped(aux))
        case other =>
          throw new UnknownEvent(version, op, errMsg(s"Unknown event $op"))
      }
    }

    def decodeRowDataUpdated(aux: Array[Byte]) =
      Delogger.RowDataUpdated(aux)(rowCodecFactory())

    def decodeCounterUpdated(aux: Array[Byte]) = {
      val msg = LogData.CounterUpdated.parseFrom(aux)
      Delogger.CounterUpdated(msg.nextCounter)
    }

    def decodeColumnCreated(aux: Array[Byte]) = {
      val msg = LogData.ColumnCreated.parseFrom(aux)
      Delogger.ColumnCreated(convert(msg.columnInfo))
    }

    def decodeColumnRemoved(aux: Array[Byte]) = {
      val msg = LogData.ColumnRemoved.parseFrom(aux)
      Delogger.ColumnRemoved(convert(msg.columnInfo))
    }

    def decodeComputationStrategyCreated(aux: Array[Byte]) = {
      val msg = LogData.ComputationStrategyCreated.parseFrom(aux)
      Delogger.ComputationStrategyCreated(convert(msg.columnInfo))
    }

    def decodeComputationStrategyRemoved(aux: Array[Byte]) = {
      val msg = LogData.ComputationStrategyRemoved.parseFrom(aux)
      Delogger.ComputationStrategyRemoved(convert(msg.columnInfo))
    }

    def decodeFieldNameUpdated(aux: Array[Byte]) = {
      val msg = LogData.FieldNameUpdated.parseFrom(aux)
      Delogger.FieldNameUpdated(convert(msg.columnInfo))
    }

    def decodeRowIdentifierSet(aux: Array[Byte]) = {
      val msg = LogData.RowIdentifierSet.parseFrom(aux)
      Delogger.RowIdentifierSet(convert(msg.columnInfo))
    }

    def decodeLastModifiedChanged(aux: Array[Byte]) = {
      val msg = LogData.LastModifiedChanged.parseFrom(aux)
      Delogger.LastModifiedChanged(convert(msg.lastModified))
    }

    def decodeRowIdentifierCleared(aux: Array[Byte]) = {
      val msg = LogData.RowIdentifierCleared.parseFrom(aux)
      Delogger.RowIdentifierCleared(convert(msg.columnInfo))
    }

    def decodeSystemRowIdentifierChanged(aux: Array[Byte]) = {
      val msg = LogData.SystemIdColumnSet.parseFrom(aux)
      Delogger.SystemRowIdentifierChanged(convert(msg.columnInfo))
    }

    def decodeVersionColumnChanged(aux: Array[Byte]) = {
      val msg = LogData.VersionColumnSet.parseFrom(aux)
      Delogger.VersionColumnChanged(convert(msg.columnInfo))
    }

    def decodeWorkingCopyCreated(aux: Array[Byte]) = {
      val msg = LogData.WorkingCopyCreated.parseFrom(aux)
      Delogger.WorkingCopyCreated(
        convert(msg.datasetInfo),
        convert(msg.copyInfo)
      )
    }

    def decodeSnapshotDropped(aux: Array[Byte]) = {
      val msg = LogData.SnapshotDropped.parseFrom(aux)
      Delogger.SnapshotDropped(convert(msg.copyInfo))
    }

    def decodeRollupCreatedOrUpdated(aux: Array[Byte]) = {
      val msg = LogData.RollupCreatedOrUpdated.parseFrom(aux)
      Delogger.RollupCreatedOrUpdated(convert(msg.rollupInfo))
    }

    def decodeRollupDropped(aux: Array[Byte]) = {
      val msg = LogData.RollupDropped.parseFrom(aux)
      Delogger.RollupDropped(convert(msg.rollupInfo))
    }

    def decodeRowsChangedPreview(aux: Array[Byte]) = {
      val msg = LogData.RowsChangedPreview.parseFrom(aux)
      Delogger.RowsChangedPreview(msg.rowsInserted, msg.rowsUpdated, msg.rowsDeleted, msg.truncated)
    }

    def decodeIndexDirectiveCreatedOrUpdated(aux: Array[Byte]) = {
      val msg = LogData.IndexDirectiveCreatedOrUpdated.parseFrom(aux)
      val enabled = Map("enabled" -> JBoolean(msg.enabled))
      val directive = JObject(msg.search match {
        case Some(b) => enabled ++ Map("search" -> JBoolean(b))
        case None => enabled
      })
      Delogger.IndexDirectiveCreatedOrUpdated(convert(msg.columnInfo), directive)
    }

    def decodeIndexDirectiveDropped(aux: Array[Byte]) = {
      val msg = LogData.IndexDirectiveDropped.parseFrom(aux)
      Delogger.IndexDirectiveDropped(convert(msg.columnInfo))
    }

    def decodeIndexCreatedOrUpdated(aux: Array[Byte]) = {
      val msg = LogData.IndexCreatedOrUpdated.parseFrom(aux)
      Delogger.IndexCreatedOrUpdated(convert(msg.indexInfo))
    }

    def decodeIndexDropped(aux: Array[Byte]) = {
      val msg = LogData.IndexDropped.parseFrom(aux)
      Delogger.IndexDropped(new IndexName(msg.name))
    }
  }

  def close() {
    _queryStmt = null
    _queryTypesStmt = null
    rs.close()
  }
}
