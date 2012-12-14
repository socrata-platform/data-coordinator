package com.socrata.datacoordinator
package truth.loader.sql

import scala.io.Codec

import java.sql.{ResultSet, PreparedStatement, Connection}

import com.socrata.datacoordinator.truth.RowLogCodec
import com.socrata.datacoordinator.truth.loader.Delogger
import com.socrata.datacoordinator.util.{CloseableIterator, LeakDetect}
import com.rojoma.json.io.JsonReader
import com.rojoma.json.ast.{JNull, JString, JObject}
import java.io.ByteArrayInputStream
import collection.immutable.VectorBuilder

class SqlDelogger[CT, CV](connection: Connection,
                          sqlizer: DataSqlizer[CT, CV],
                          rowCodecFactory: () => RowLogCodec[CV])
  extends Delogger[CT, CV]
{
  var stmt: PreparedStatement = null

  def query = {
    if(stmt == null) {
      stmt = connection.prepareStatement("select subversion, what, aux from " + sqlizer.logTableName + " where version = ? order by subversion")
      stmt.setFetchSize(1)
    }
    stmt
  }

  def delog(version: Long) = {
    new LogIterator(version) with LeakDetect
  }

  class LogIterator(version: Long) extends CloseableIterator[Delogger.LogEvent[CT, CV]] {
    var rs: ResultSet = null
    var lastSubversion = 0L
    var nextResult: Delogger.LogEvent[CT, CV] = null
    var done = false
    val UTF8 = Codec.UTF8

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
        done
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
      assert(subversion == lastSubversion + 1, "subversion skipped?")
      lastSubversion = subversion

      val op = rs.getString("what")
      val aux = rs.getBytes("aux")

      nextResult = op match {
        case SqlLogger.RowDataUpdated =>
          decodeRowDataUpdated(aux)
        case SqlLogger.Truncated =>
          decodeTruncated(aux)
        case SqlLogger.ColumnCreated =>
          decodeColumnCreated(aux)
        case SqlLogger.ColumnRemoved =>
          decodeColumnRemoved(aux)
        case SqlLogger.RowIdentifierChanged =>
          decodeRowIdentifierChanged(aux)
        case SqlLogger.WorkingCopyCreated =>
          Delogger.WorkingCopyCreated
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

      val results = new VectorBuilder[Delogger.Operation[CV]]
      def loop(): Vector[Delogger.Operation[CV]] = {
        codec.extract(cis) match {
          case Some(op) =>
            // Ick.  TODO: merge these two Operation types
            op match {
              case RowLogCodec.Insert(sid, row) =>
                results += Delogger.Insert(sid, row)
              case RowLogCodec.Update(sid, row) =>
                results += Delogger.Update(sid, row)
              case RowLogCodec.Delete(sid) =>
                results += Delogger.Delete(sid)
            }
            loop()
          case None =>
            results.result()
        }
      }
      Delogger.RowDataUpdated(loop())
    }

    def decodeTruncated(aux: Array[Byte]) = {
      val json = fromJson(aux).cast[JObject].getOrElse {
        sys.error("Parameter for `truncated' was not an object")
      }
      val schema = Map.newBuilder[ColumnId, CT]
      try {
        for((k, v) <- json) {
          val cv = v.cast[JString].getOrElse {
            sys.error("value in truncated was not a string")
          }
          schema += k.toLong -> sqlizer.typeContext.typeFromName(cv.string)
        }
      } catch {
        case _: NumberFormatException =>
          sys.error("key in truncated was not a valid column id")
      }
      Delogger.Truncated(schema.result())
    }

    def decodeColumnCreated(aux: Array[Byte]) = {
      val json = fromJson(aux).cast[JObject].getOrElse {
        sys.error("Parameter for `column created' was not an object")
      }
      val name = getString("c", json)
      val typ = getString("t", json)

      Delogger.ColumnCreated(name, sqlizer.typeContext.typeFromName(typ))
    }

    def decodeColumnRemoved(aux: Array[Byte]) = {
      val json = fromJson(aux).cast[JObject].getOrElse {
        sys.error("Parameter for `column created' was not an object")
      }
      val name = getString("c", json)

      Delogger.ColumnRemoved(name)
    }

    def decodeRowIdentifierChanged(aux: Array[Byte]) = {
      val json = fromJson(aux).cast[JObject].getOrElse {
        sys.error("Parameter for `column created' was not an object")
      }
      val newCol = json.get("c") match {
        case Some(JString(c)) => Some(c)
        case Some(JNull) => None
        case Some(_) => sys.error("Parameter `c' was not a string or null")
        case None => sys.error("Parameter `c' did not exist")
      }

      Delogger.RowIdentifierChanged(newCol)
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
