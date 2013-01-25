package com.socrata.datacoordinator
package truth.loader.sql

import scala.io.Codec
import scala.collection.immutable.VectorBuilder

import java.sql.{ResultSet, PreparedStatement, Connection}
import java.io.ByteArrayInputStream

import com.rojoma.json.io.JsonReader
import com.rojoma.json.ast.{JNumber, JNull, JString, JObject}

import com.socrata.datacoordinator.truth.RowLogCodec
import com.socrata.datacoordinator.truth.loader.{Operation, Delogger}
import com.socrata.datacoordinator.util.{CloseableIterator, LeakDetect}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}
import com.rojoma.json.codec.JsonCodec
import com.socrata.datacoordinator.util.collection.MutableColumnIdMap
import com.socrata.datacoordinator.id.{RowId, ColumnId}

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
        case SqlLogger.Truncated =>
          decodeTruncated(aux)
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
        case SqlLogger.WorkingCopyPublished =>
          Delogger.WorkingCopyPublished
        case SqlLogger.TransactionEnded =>
          assert(!rs.next(), "there was data after TransactionEnded?")
          null
        case other =>
          sys.error("Unknown operation " + op)
      }
    }

    def decodeRowDataUpdated(aux: Array[Byte]) = {
      val codec = rowCodecFactory()

      val bais = new ByteArrayInputStream(aux)
      val sis = new org.xerial.snappy.SnappyInputStream(bais)
      val cis = com.google.protobuf.CodedInputStream.newInstance(sis)

      // TODO: dispatch on version (right now we have only one)
      codec.skipVersion(cis)

      val results = new VectorBuilder[Operation[CV]]
      def loop(): Vector[Operation[CV]] = {
        codec.extract(cis) match {
          case Some(op) =>
            results += op
            loop()
          case None =>
            results.result()
        }
      }
      Delogger.RowDataUpdated(loop())
    }

    def decodeRowIdCounterUpdated(aux: Array[Byte]) = {
      val json = fromJson(aux).cast[JNumber].getOrElse {
        sys.error("Parameter for `row id counter updated' was not a number")
      }
      Delogger.RowIdCounterUpdated(new RowId(json.toLong))
    }

    def decodeTruncated(aux: Array[Byte]) = {
      val json = fromJson(aux).cast[JObject].getOrElse {
        sys.error("Parameter for `truncated' was not an object")
      }
      val schema = new MutableColumnIdMap[ColumnInfo]
      try {
        for((k, v) <- json) {
          val ci = JsonCodec.fromJValue[ColumnInfo](v).getOrElse {
            sys.error("value in truncated was not a ColumnInfo")
          }
          schema(new ColumnId(k.toLong)) -> ci
        }
      } catch {
        case _: NumberFormatException =>
          sys.error("key in truncated was not a valid column id")
      }
      Delogger.Truncated(schema.freeze())
    }

    def decodeColumnCreated(aux: Array[Byte]) = {
      val ci = JsonCodec.fromJValue[ColumnInfo](fromJson(aux)).getOrElse {
        sys.error("Parameter for `column created' was not a ColumnInfo")
      }

      Delogger.ColumnCreated(ci)
    }

    def decodeColumnRemoved(aux: Array[Byte]) = {
      val ci = JsonCodec.fromJValue[ColumnInfo](fromJson(aux)).getOrElse {
        sys.error("Parameter for `column created' was not an object")
      }
      Delogger.ColumnRemoved(ci)
    }

    def decodeRowIdentifierSet(aux: Array[Byte]) = {
      val ci = JsonCodec.fromJValue[ColumnInfo](fromJson(aux)).getOrElse {
        sys.error("Parameter for `row identifier set' was not an object")
      }
      Delogger.RowIdentifierSet(ci)
    }

    def decodeRowIdentifierCleared(aux: Array[Byte]) = {
      val ci = JsonCodec.fromJValue[ColumnInfo](fromJson(aux)).getOrElse {
        sys.error("Parameter for `row identifier cleared' was not an object")
      }
      Delogger.RowIdentifierCleared(ci)
    }

    def decodeSystemRowIdentifierChanged(aux: Array[Byte]) = {
      val ci = JsonCodec.fromJValue[ColumnInfo](fromJson(aux)).getOrElse {
        sys.error("Parameter for `system row identifier changed' was not an object")
      }
      Delogger.SystemRowIdentifierChanged(ci)
    }

    def decodeWorkingCopyCreated(aux: Array[Byte]) = {
      val vi = JsonCodec.fromJValue[CopyInfo](fromJson(aux)).getOrElse {
        sys.error("Parameter for `working copy created' was not an object")
      }
      Delogger.WorkingCopyCreated(vi)
    }

    def fromJson(aux: Array[Byte]) = JsonReader.fromString(new String(aux, UTF8))

    def getString(param: String, o: JObject): String =
      o.getOrElse(param, sys.error("Parameter `" + param + "' did not exist")).cast[JString].getOrElse {
        sys.error("Parameter `" + param + "' was not a string")
      }.string
  }

  def close() {
    if(stmt != null) { stmt.close(); stmt = null }
  }
}
