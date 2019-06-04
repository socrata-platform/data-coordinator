package com.socrata.datacoordinator
package truth.loader.sql

import scala.io.Codec

import java.sql.{ResultSet, PreparedStatement, Connection}

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.RowLogCodec
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.util.{CloseableIterator, LeakDetect}
import com.socrata.soql.environment.ColumnName

class SqlDelogger[CV](connection: Connection,
                      logTableName: String,
                      rowCodecFactory: () => RowLogCodec[CV])
  extends Delogger[CV]
{
  import messages.FromProtobuf._

  require(!connection.getAutoCommit, "connection is in auto-commit mode") // need non-AC in order to do streaming reads

  var stmt: PreparedStatement = null

  def query = {
    if(stmt == null) {
      stmt = connection.prepareStatement("select subversion, what, aux from " + logTableName + " where version = ? order by subversion")
      stmt.setFetchSize(1)
    }
    stmt
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

  class LogIterator(version: Long) extends CloseableIterator[Delogger.LogEvent[CV]] {
    var rs: ResultSet = null
    var lastSubversion = 0L
    var nextResult: Delogger.LogEvent[CV] = null
    var done = false
    val UTF8 = Codec.UTF8.charSet

    def errMsg(m: String) = m + " in version " + version + " of log " + logTableName

    override def toString() = {
      // this exists because otherwise Iterator#toString calls hasNext.
      // Since this hasNext has side-effects, it causes unpredictable
      // behaviour when tracing through it and IDEA tries to call
      // toString on "this".
      val state = if(!done) "unfinished" else "finished"
      s"LogIterator($state)"
    }

    def advance() {
      nextResult = null
      if(rs == null) {
        query.setLong(1, version)
        rs = query.executeQuery()
      }
      decodeNext()
      done = nextResult == null
    }

    def hasNext = {
      if(done) false
      else {
        if(nextResult == null) advance()
        !done
      }
    }

    def next() =
      if(hasNext) {
        val r = nextResult
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
      val aux = rs.getBytes("aux")

      nextResult = op match {
        case SqlLogger.RowDataUpdated =>
          decodeRowDataUpdated(aux)
        case SqlLogger.CounterUpdated =>
          decodeCounterUpdated(aux)
        case SqlLogger.ColumnCreated =>
          decodeColumnCreated(aux)
        case SqlLogger.ColumnRemoved =>
          decodeColumnRemoved(aux)
        case SqlLogger.ComputationStrategyCreated =>
          decodeComputationStrategyCreated(aux)
        case SqlLogger.ComputationStrategyRemoved =>
          decodeComputationStrategyRemoved(aux)
        case SqlLogger.FieldNameUpdated =>
          decodeFieldNameUpdated(aux)
        case SqlLogger.RowIdentifierSet =>
          decodeRowIdentifierSet(aux)
        case SqlLogger.RowIdentifierCleared =>
          decodeRowIdentifierCleared(aux)
        case SqlLogger.SystemRowIdentifierChanged =>
          decodeSystemRowIdentifierChanged(aux)
        case SqlLogger.VersionColumnChanged =>
          decodeVersionColumnChanged(aux)
        case SqlLogger.LastModifiedChanged =>
          decodeLastModifiedChanged(aux)
        case SqlLogger.WorkingCopyCreated =>
          decodeWorkingCopyCreated(aux)
        case SqlLogger.DataCopied =>
          Delogger.DataCopied
        case SqlLogger.WorkingCopyDropped =>
          Delogger.WorkingCopyDropped
        case SqlLogger.SnapshotDropped =>
          decodeSnapshotDropped(aux)
        case SqlLogger.WorkingCopyPublished =>
          Delogger.WorkingCopyPublished
        case SqlLogger.Truncated =>
          Delogger.Truncated
        case SqlLogger.RollupCreatedOrUpdated =>
          decodeRollupCreatedOrUpdated(aux)
        case SqlLogger.RollupDropped =>
          decodeRollupDropped(aux)
        case SqlLogger.RowsChangedPreview =>
          decodeRowsChangedPreview(aux)
        case SqlLogger.TransactionEnded =>
          assert(!rs.next(), "there was data after TransactionEnded?")
          null
        case SqlLogger.SecondaryReindex =>
          Delogger.SecondaryReindex
        case SqlLogger.SecondaryAddIndex =>
          decodeSecondaryAddIndex(aux)
        case other =>
          throw new UnknownEvent(version, op, errMsg(s"Unknown event $op"))
      }
    }

    def decodeRowDataUpdated(aux: Array[Byte]) =
      Delogger.RowDataUpdated(aux)(rowCodecFactory())

    def decodeCounterUpdated(aux: Array[Byte]) = {
      val msg = messages.CounterUpdated.defaultInstance.mergeFrom(aux)
      Delogger.CounterUpdated(msg.nextCounter)
    }

    def decodeColumnCreated(aux: Array[Byte]) = {
      val msg = messages.ColumnCreated.defaultInstance.mergeFrom(aux)
      Delogger.ColumnCreated(convert(msg.columnInfo))
    }

    def decodeColumnRemoved(aux: Array[Byte]) = {
      val msg = messages.ColumnRemoved.defaultInstance.mergeFrom(aux)
      Delogger.ColumnRemoved(convert(msg.columnInfo))
    }

    def decodeComputationStrategyCreated(aux: Array[Byte]) = {
      val msg = messages.ComputationStrategyCreated.defaultInstance.mergeFrom(aux)
      Delogger.ComputationStrategyCreated(convert(msg.columnInfo))
    }

    def decodeComputationStrategyRemoved(aux: Array[Byte]) = {
      val msg = messages.ComputationStrategyRemoved.defaultInstance.mergeFrom(aux)
      Delogger.ComputationStrategyRemoved(convert(msg.columnInfo))
    }

    def decodeFieldNameUpdated(aux: Array[Byte]) = {
      val msg = messages.FieldNameUpdated.defaultInstance.mergeFrom(aux)
      Delogger.FieldNameUpdated(convert(msg.columnInfo))
    }

    def decodeRowIdentifierSet(aux: Array[Byte]) = {
      val msg = messages.RowIdentifierSet.defaultInstance.mergeFrom(aux)
      Delogger.RowIdentifierSet(convert(msg.columnInfo))
    }

    def decodeLastModifiedChanged(aux: Array[Byte]) = {
      val msg = messages.LastModifiedChanged.defaultInstance.mergeFrom(aux)
      Delogger.LastModifiedChanged(convert(msg.lastModified))
    }

    def decodeRowIdentifierCleared(aux: Array[Byte]) = {
      val msg = messages.RowIdentifierCleared.defaultInstance.mergeFrom(aux)
      Delogger.RowIdentifierCleared(convert(msg.columnInfo))
    }

    def decodeSystemRowIdentifierChanged(aux: Array[Byte]) = {
      val msg = messages.SystemIdColumnSet.defaultInstance.mergeFrom(aux)
      Delogger.SystemRowIdentifierChanged(convert(msg.columnInfo))
    }

    def decodeVersionColumnChanged(aux: Array[Byte]) = {
      val msg = messages.VersionColumnSet.defaultInstance.mergeFrom(aux)
      Delogger.VersionColumnChanged(convert(msg.columnInfo))
    }

    def decodeWorkingCopyCreated(aux: Array[Byte]) = {
      val msg = messages.WorkingCopyCreated.defaultInstance.mergeFrom(aux)
      Delogger.WorkingCopyCreated(
        convert(msg.datasetInfo),
        convert(msg.copyInfo)
      )
    }

    def decodeSnapshotDropped(aux: Array[Byte]) = {
      val msg = messages.SnapshotDropped.defaultInstance.mergeFrom(aux)
      Delogger.SnapshotDropped(convert(msg.copyInfo))
    }

    def decodeRollupCreatedOrUpdated(aux: Array[Byte]) = {
      val msg = messages.RollupCreatedOrUpdated.defaultInstance.mergeFrom(aux)
      Delogger.RollupCreatedOrUpdated(convert(msg.rollupInfo))
    }

    def decodeRollupDropped(aux: Array[Byte]) = {
      val msg = messages.RollupDropped.defaultInstance.mergeFrom(aux)
      Delogger.RollupDropped(convert(msg.rollupInfo))
    }

    def decodeRowsChangedPreview(aux: Array[Byte]) = {
      val msg = messages.RowsChangedPreview.defaultInstance.mergeFrom(aux)
      Delogger.RowsChangedPreview(msg.rowsInserted, msg.rowsUpdated, msg.rowsDeleted, msg.truncated)
    }

    def decodeSecondaryAddIndex(aux: Array[Byte]) = {
      val msg = messages.SecondaryAddIndex.defaultInstance.mergeFrom(aux)
      Delogger.SecondaryAddIndex(ColumnName(msg.`fieldName`))
    }
  }

  def close() {
    if(stmt != null) { stmt.close(); stmt = null }
  }
}
