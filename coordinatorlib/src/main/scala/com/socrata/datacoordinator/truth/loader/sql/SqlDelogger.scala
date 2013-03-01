package com.socrata.datacoordinator
package truth.loader.sql

import scala.io.Codec

import java.io.StringReader
import java.sql.{ResultSet, PreparedStatement, Connection}

import com.rojoma.json.util.JsonUtil
import com.rojoma.json.codec.JsonCodec
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.RowLogCodec
import com.socrata.datacoordinator.truth.loader.Delogger
import com.socrata.datacoordinator.util.{CloseableIterator, LeakDetect}
import com.socrata.datacoordinator.truth.metadata.{UnanchoredDatasetInfo, UnanchoredCopyInfo, UnanchoredColumnInfo}
import com.socrata.datacoordinator.id.RowId

class SqlDelogger[CV](connection: Connection,
                      logTableName: String,
                      rowCodecFactory: () => RowLogCodec[CV])
  extends Delogger[CV]
{
  var stmt: PreparedStatement = null

  def query = {
    if(stmt == null) {
      stmt = connection.prepareStatement("select subversion, what, aux from " + logTableName + " where version = ? order by subversion")
      stmt.setFetchSize(1)
    }
    stmt
  }

  def findEndOfWorkingCopy(fromVersion: Long): Option[Long] = {
    using(connection.prepareStatement("select version from " + logTableName + " where version >= ? and subversion = 1 and what in (?, ?) order by version limit 1")) { stmt =>
      stmt.setLong(1, fromVersion)
      stmt.setString(2, SqlLogger.WorkingCopyPublished)
      stmt.setString(3, SqlLogger.WorkingCopyDropped)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          Some(rs.getLong("version"))
        } else {
          None
        }
      }
    }
  }

  def delog(version: Long) = {
    new LogIterator(version) with LeakDetect
  }

  class LogIterator(version: Long) extends CloseableIterator[Delogger.LogEvent[CV]] {
    var rs: ResultSet = null
    var lastSubversion = 0L
    var nextResult: Delogger.LogEvent[CV] = null
    var done = false
    val UTF8 = Codec.UTF8.charSet

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
      if(rs.next()) decode()
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

    def decode() {
      val subversion = rs.getLong("subversion")
      assert(subversion == lastSubversion + 1, s"subversion skipped?  Got $subversion expected ${lastSubversion + 1}")
      lastSubversion = subversion

      val op = rs.getString("what")
      val aux = rs.getBytes("aux")

      nextResult = op match {
        case SqlLogger.RowDataUpdated =>
          decodeRowDataUpdated(aux)
        case SqlLogger.RowIdCounterUpdated =>
          decodeRowIdCounterUpdated(aux)
        case SqlLogger.ColumnCreated =>
          decodeColumnCreated(aux)
        case SqlLogger.ColumnRemoved =>
          decodeColumnRemoved(aux)
        case SqlLogger.RowIdentifierSet =>
          decodeRowIdentifierSet(aux)
        case SqlLogger.RowIdentifierCleared =>
          decodeRowIdentifierCleared(aux)
        case SqlLogger.SystemRowIdentifierChanged =>
          decodeSystemRowIdentifierChanged(aux)
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
        case SqlLogger.ColumnLogicalNameChanged =>
          decodeColumnLogicalNameChanged(aux)
        case SqlLogger.Truncated =>
          Delogger.Truncated
        case SqlLogger.TransactionEnded =>
          assert(!rs.next(), "there was data after TransactionEnded?")
          null
        case other =>
          sys.error("Unknown operation " + op)
      }
    }

    def decodeRowDataUpdated(aux: Array[Byte]) =
      Delogger.RowDataUpdated(aux)(rowCodecFactory())

    def decodeRowIdCounterUpdated(aux: Array[Byte]) = {
      val rid = fromJson[RowId](aux).getOrElse {
        sys.error("Parameter for `row id counter updated' was not a number")
      }
      Delogger.RowIdCounterUpdated(rid)
    }

    def decodeColumnCreated(aux: Array[Byte]) = {
      val ci = fromJson[UnanchoredColumnInfo](aux).getOrElse {
        sys.error("Parameter for `column created' was not a ColumnInfo")
      }

      Delogger.ColumnCreated(ci)
    }

    def decodeColumnRemoved(aux: Array[Byte]) = {
      val ci = fromJson[UnanchoredColumnInfo](aux).getOrElse {
        sys.error("Parameter for `column created' was not an object")
      }
      Delogger.ColumnRemoved(ci)
    }

    def decodeRowIdentifierSet(aux: Array[Byte]) = {
      val ci = fromJson[UnanchoredColumnInfo](aux).getOrElse {
        sys.error("Parameter for `row identifier set' was not an object")
      }
      Delogger.RowIdentifierSet(ci)
    }

    def decodeRowIdentifierCleared(aux: Array[Byte]) = {
      val ci = fromJson[UnanchoredColumnInfo](aux).getOrElse {
        sys.error("Parameter for `row identifier cleared' was not an object")
      }
      Delogger.RowIdentifierCleared(ci)
    }

    def decodeSystemRowIdentifierChanged(aux: Array[Byte]) = {
      val ci = fromJson[UnanchoredColumnInfo](aux).getOrElse {
        sys.error("Parameter for `system row identifier changed' was not an object")
      }
      Delogger.SystemRowIdentifierChanged(ci)
    }

    def decodeWorkingCopyCreated(aux: Array[Byte]) = {
      val json = new StringReader(new String(aux, UTF8))
      val di = JsonUtil.readJson[UnanchoredDatasetInfo](json).getOrElse {
        sys.error("First parameter for `working copy created' was not an object")
      }
      val vi = JsonUtil.readJson[UnanchoredCopyInfo](json).getOrElse {
        sys.error("Second parameter for `working copy created' was not an object")
      }
      Delogger.WorkingCopyCreated(di, vi)
    }

    def decodeSnapshotDropped(aux: Array[Byte]) = {
      val json = new StringReader(new String(aux, UTF8))
      val vi = JsonUtil.readJson[UnanchoredCopyInfo](json).getOrElse {
        sys.error("Parameter for `working copy dropped' was not an object")
      }
      Delogger.SnapshotDropped(vi)
    }

    def decodeColumnLogicalNameChanged(aux: Array[Byte]) = {
      val json = new StringReader(new String(aux, UTF8))
      val ci = JsonUtil.readJson[UnanchoredColumnInfo](json).getOrElse {
        sys.error("Parameter for `column logical name changed' was not an object")
      }
      Delogger.ColumnLogicalNameChanged(ci)
    }

    def fromJson[T : JsonCodec](aux: Array[Byte]): Option[T] = JsonUtil.parseJson[T](new String(aux, UTF8))
  }

  def close() {
    if(stmt != null) { stmt.close(); stmt = null }
  }
}
