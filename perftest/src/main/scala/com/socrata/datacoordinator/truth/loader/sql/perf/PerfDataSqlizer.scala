package com.socrata.datacoordinator
package truth.loader
package sql
package perf

import java.sql.{Connection, PreparedStatement}

import org.postgresql.core.BaseConnection
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.util.{FastGroupedIterator, CloseableIterator, StringBuilderReader}
import com.socrata.datacoordinator.truth.DatasetContext

class PerfDataSqlizer(tableBase: String, val datasetContext: DatasetContext[PerfType, PerfValue]) extends DataSqlizer[PerfType, PerfValue] {
  val dataTableName = tableBase + "_data"
  val logTableName = tableBase + "_log"

  def typeContext = PerfTypeContext

  def sizeof(x: Long) = 8
  def sizeof(s: String) = s.length << 1
  def sizeof(bd: BigDecimal) = bd.precision
  def sizeofNull = 1

  def softMaxBatchSize = 2000000

  def sizerFrom(base: Int, t: PerfType): PerfValue => Int = t match {
    case PTId => {
      case PVId(v) => base+8
      case PVNull => base+4
      case other => sys.error("Unexpected value for type " + t + other.getClass.getSimpleName)
    }
    case PTNumber => {
      case PVNumber(v) => base+v.precision
      case PVNull => base+4
      case other => sys.error("Unexpected value for type " + t + other.getClass.getSimpleName)
    }
    case PTText => {
      case PVText(v) => base+v.length
      case PVNull => base+4
      case other => sys.error("Unexpected value for type " + t + other.getClass.getSimpleName)
    }
  }
  def updateSizerForType(c: String, t: PerfType) = sizerFrom(c.length, t)
  def insertSizerForType(c: String, t: PerfType) = sizerFrom(0, t)

  val updateSizes = datasetContext.fullSchema.map { case (c, t) =>
    c -> updateSizerForType(c, t)
  }

  val insertSizes = datasetContext.fullSchema.map { case (c, t) =>
    c -> insertSizerForType(c, t)
  }

  def sizeofDelete = 8

  val baseUpdateSize = 50
  def sizeofUpdate(row: Row[PerfValue]) =
    row.foldLeft(baseUpdateSize) { (total, cv) =>
      val (c,v) = cv
      total + updateSizes(c)(v)
    }

  def sizeofInsert(row: Row[PerfValue]) =
    row.foldLeft(0) { (total, cv) =>
      val (c,v) = cv
      total + insertSizes(c)(v)
    }

  val mapToPhysical = Map.empty[String, String] ++ datasetContext.systemSchema.keys.map { k => k -> k.substring(1) } ++ datasetContext.userSchema.keys.map { k =>
    k -> ("u_" + k)
  }

  val logicalColumns = datasetContext.fullSchema.keys.toArray
  val physicalColumns = logicalColumns.map(mapToPhysical)

  val pkCol = mapToPhysical(datasetContext.primaryKeyColumn)

  val prepareSystemIdDeleteStatement =
    "DELETE FROM " + dataTableName + " WHERE id = ?"

  val bulkInsertStatement =
    "COPY " + dataTableName + " (" + physicalColumns.mkString(",") + ") from stdin with csv"

  def insertBatch(conn: Connection)(f: Inserter => Unit): Long = {
    val inserter = new InserterImpl
    f(inserter)
    val copyManager = conn.asInstanceOf[BaseConnection].getCopyAPI
    copyManager.copyIn(bulkInsertStatement, inserter.reader)
  }

  class InserterImpl extends Inserter {
    val sb = new java.lang.StringBuilder
    def insert(sid: Long, row: Row[PerfValue]) {
      var didOne = false
      for(k <- logicalColumns) {
        if(didOne) sb.append(',')
        else didOne = true
        csvize(sb, k, row.getOrElse(k, PVNull))
      }
      sb.append('\n')
    }

    def close() {}

    def reader: java.io.Reader = new StringBuilderReader(sb)
  }

  def csvize(sb: java.lang.StringBuilder, k: String, v: PerfValue) = {
    v match {
      case PVText(s) =>
        sb.append('"')
        var i = 0
        val limit = s.length
        while(i != limit) {
          val c = sb.charAt(i)
          sb.append(c)
          if(c == '"') sb.append('"')
          i += 1
        }
        sb.append('"')
      case PVNumber(n) =>
        sb.append(n)
      case PVId(n) =>
        sb.append(n)
      case PVNull =>
        // nothing
    }
  }

  def prepareSystemIdDelete(stmt: PreparedStatement, sid: Long) = {
    stmt.setLong(1, sid)
    sizeof(sid)
  }

  def prepareSystemIdInsert(stmt: PreparedStatement, sid: Long, row: Row[PerfValue]) = {
    var totalSize = 0
    var i = 0
    while(i != logicalColumns.length) {
      totalSize += setValue(stmt, i + 1, datasetContext.fullSchema(logicalColumns(i)), row.getOrElse(logicalColumns(i), PVNull))
      i += 1
    }
    stmt.setLong(i + 1, sid)
    totalSize += sizeof(sid)

    totalSize
  }

  def sqlizeSystemIdUpdate(sid: Long, row: Row[PerfValue]) =
    "UPDATE " + dataTableName + " SET " + row.map { case (col, v) => mapToPhysical(col) + " = " + v.sqlize }.mkString(",") + " WHERE id = " + sid

  val prepareUserIdDeleteStatement =
    "DELETE FROM " + dataTableName + " WHERE " + pkCol + " = ?"

  def prepareUserIdDelete(stmt: PreparedStatement, id: PerfValue) = {
    setValue(stmt, 1, datasetContext.userSchema(datasetContext.userPrimaryKeyColumn.get), id)
  }

  def setValue(stmt: PreparedStatement, col: Int, typ: PerfType, v: PerfValue): Int = {
    v match {
      case PVId(x) => stmt.setLong(col, x); sizeof(x)
      case PVNumber(x) => stmt.setBigDecimal(col, x.underlying); sizeof(x)
      case PVText(x) => stmt.setString(col, x); sizeof(x)
      case PVNull =>
        typ match {
          case PTId => stmt.setNull(col, java.sql.Types.INTEGER)
          case PTNumber => stmt.setNull(col, java.sql.Types.NUMERIC)
          case PTText => stmt.setNull(col, java.sql.Types.VARCHAR)
        }
        sizeofNull
    }
  }

  def sqlizeUserIdUpdate(row: Row[PerfValue]) =
    "UPDATE " + dataTableName + " SET " + row.map { case (col, v) => mapToPhysical(col) + " = " + v.sqlize }.mkString(",") + " WHERE " + pkCol + " = " + row(datasetContext.primaryKeyColumn).sqlize

  val findSystemIdsPrefix = "SELECT id as sid, " + pkCol + " as uid FROM " + dataTableName + " WHERE "

  def findSystemIds(conn: Connection, ids: Iterator[PerfValue]): CloseableIterator[Seq[IdPair[PerfValue]]] = {
    val typ = datasetContext.userSchema(datasetContext.userPrimaryKeyColumn.getOrElse(sys.error("findSystemIds called without a user primary key")))

    new CloseableIterator[Seq[IdPair[PerfValue]]] {
      val blockSize = 100
      val grouped = new FastGroupedIterator(ids, blockSize)
      val stmt = conn.prepareStatement(findSystemIdsPrefix + (1 to blockSize).map(_ => "?").mkString(pkCol + " in (", ",", ")"))

      def hasNext = grouped.hasNext

      def next(): Seq[IdPair[PerfValue]] = {
        val block = grouped.next()
        if(block.size != blockSize) {
          using(conn.prepareStatement(findSystemIdsPrefix + (1 to block.size).map(_ => "?").mkString(pkCol + " in (", ",", ")"))) { stmt2 =>
            fillAndResult(stmt2, block)
          }
        } else {
          fillAndResult(stmt, block)
        }
      }

      def fillAndResult(stmt: PreparedStatement, block: Seq[PerfValue]): Seq[IdPair[PerfValue]] = {
        var i = 1
        for(cv <- block) {
          setValue(stmt, i, typ, cv)
          i += 1
        }
        using(stmt.executeQuery()) { rs =>
          val extractor = typ match {
            case PTNumber =>
              { () =>
                val l = rs.getBigDecimal("uid")
                if(l == null) PVNull
                else PVNumber(l)
              }
            case PTText =>
              { () =>
                val s = rs.getString("uid")
                if(s == null) PVNull
                else PVText(s)
              }
            case PTId =>
              { () =>
                val l = rs.getLong("uid")
                if(rs.wasNull) PVNull
                else PVId(l)
              }
          }

          val result = new scala.collection.mutable.ArrayBuffer[IdPair[PerfValue]](block.length)
          while(rs.next()) {
            val sid = rs.getLong(1)
            val uid = extractor()
            result += IdPair(sid, uid)
          }
          result
        }
      }

      def close() { stmt.close() }
    }
  }
  // This may batch the "ids" into multiple queries.  The queries
  def findSystemIds(ids: Iterator[PerfValue]) = {
    require(datasetContext.hasUserPrimaryKey, "findSystemIds called without a user primary key")
    ids.grouped(100).map { block =>
      block.map(_.sqlize).mkString("SELECT id AS sid, " + pkCol + " AS uid FROM " + dataTableName + " WHERE " + pkCol + " IN (", ",", ")")
    }
  }
}
