package com.socrata.datacoordinator.loader
package loaderperf

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.rojoma.json.ast._
import com.rojoma.json.util.JsonUtil
import com.rojoma.json.codec.JsonCodec
import org.postgresql.core.BaseConnection
import org.postgresql.copy.CopyManager

class PerfDataSqlizer(user: String, val datasetContext: DatasetContext[PerfType, PerfValue]) extends PerfSqlizer(datasetContext) with DataSqlizer[PerfType, PerfValue] {
  val userSqlized = PVText(user).sqlize
  val dataTableName = datasetContext.baseName + "_data"
  val logTableName = datasetContext.baseName + "_log"

  def sizeof(x: Long) = 8
  def sizeof(s: String) = s.length << 1
  def sizeof(bd: BigDecimal) = bd.precision
  def sizeofNull = 1

  def softMaxBatchSize = 1000000

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
    val copyManager = new CopyManager(conn.asInstanceOf[BaseConnection])
    copyManager.copyIn(bulkInsertStatement, inserter.reader)
  }

  class InserterImpl extends Inserter {
    val sb = new java.lang.StringBuilder
    def insert(sid: Long, row: Row[PerfValue]) {
      val trueRow = row + (datasetContext.systemIdColumnName -> PVId(sid))
      var didOne = false
      for(k <- logicalColumns) {
        if(didOne) sb.append(',')
        else didOne = true
        csvize(sb, k, trueRow.getOrElse(k, PVNull))
      }
      sb.append('\n')
    }

    def close() {}

    def reader: java.io.Reader = new java.io.Reader {
      var srcPtr = 0
      def read(dst: Array[Char], off: Int, len: Int): Int = {
        val remaining = sb.length - srcPtr
        if(remaining == 0) return -1
        val count = java.lang.Math.min(remaining, len)
        val end = srcPtr + count
        sb.getChars(srcPtr, end, dst, off)
        srcPtr = end
        count
      }
      def close() {}
    }
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

  def prepareSystemIdInsertStatement =
    "INSERT INTO " + dataTableName + " (" + physicalColumns.mkString(",") + ") SELECT " + physicalColumns.map(_ => "?").mkString(",") + " WHERE NOT EXISTS (SELECT 1 FROM " + dataTableName + " WHERE id = ?)"

  def prepareSystemIdDelete(stmt: PreparedStatement, sid: Long) = {
    stmt.setLong(1, sid)
    sizeof(sid)
  }

  def prepareSystemIdInsert(stmt: PreparedStatement, sid: Long, row: Row[PerfValue]) = {
    var totalSize = 0
    var i = 0
    val fullRow = row + (datasetContext.systemIdColumnName -> PVId(sid))
    while(i != logicalColumns.length) {
      totalSize += setValue(stmt, i + 1, datasetContext.fullSchema(logicalColumns(i)), fullRow.getOrElse(logicalColumns(i), PVNull))
      i += 1
    }
    stmt.setLong(i + 1, sid)
    totalSize += sizeof(sid)

    totalSize
  }

  def sqlizeSystemIdUpdate(sid: Long, row: Row[PerfValue]) =
    "UPDATE " + dataTableName + " SET " + row.map { case (col, v) => mapToPhysical(col) + " = " + v.sqlize }.mkString(",") + " WHERE id = " + row(datasetContext.systemIdColumnName).sqlize

  val prepareUserIdDeleteStatement =
    "DELETE FROM " + dataTableName + " WHERE " + pkCol + " = ?"

  def prepareUserIdInsertStatement =
    "INSERT INTO " + dataTableName + " (" + physicalColumns.mkString(",") + ") SELECT " + physicalColumns.map(_ => "?").mkString(",") + " WHERE NOT EXISTS (SELECT 1 FROM " + dataTableName + " WHERE " + pkCol + " = ?)"

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

  def prepareUserIdInsert(stmt: PreparedStatement, sid: Long, row: Row[PerfValue]) = {
    var totalSize = 0
    var i = 0
    val fullRow = row + (datasetContext.systemIdColumnName -> PVId(sid))
    while(i != logicalColumns.length) {
      totalSize += setValue(stmt, i + 1, datasetContext.fullSchema(logicalColumns(i)), fullRow.getOrElse(logicalColumns(i), PVNull))
      i += 1
    }
    val c = datasetContext.userPrimaryKeyColumn.get
    totalSize += setValue(stmt, i + 1, datasetContext.userSchema(c), fullRow(c))
    totalSize
  }

  def sqlizeUserIdUpdate(row: Row[PerfValue]) =
    "UPDATE " + dataTableName + " SET " + (row - pkCol).map { case (col, v) => mapToPhysical(col) + " = " + v.sqlize }.mkString(",") + " WHERE " + pkCol + " = " + row(datasetContext.primaryKeyColumn).sqlize

  // txn log has (serial, row id, who did the update)
  val findCurrentVersion =
    "SELECT COALESCE(MAX(id), 0) FROM " + logTableName

  type LogAuxColumn = Array[Byte]

  val prepareLogRowsChangedStatement =
    "INSERT INTO " + logTableName + " (id, rows, who) VALUES (?, ?," + userSqlized + ")"

  def prepareLogRowsChanged(stmt: PreparedStatement, version: Long, rowsJson: LogAuxColumn) = {
    stmt.setLong(1, version)
    stmt.setBytes(2, rowsJson)
    sizeof(version) + rowsJson.length
  }

  def newRowAuxDataAccumulator(auxUser: (LogAuxColumn) => Unit) = new RowAuxDataAccumulator {
    var baos: java.io.ByteArrayOutputStream = _
    var out: java.io.Writer = _
    var didOne: Boolean = _
    reset()

    def reset() {
      baos = new java.io.ByteArrayOutputStream
      out = new java.io.OutputStreamWriter(new org.xerial.snappy.SnappyOutputStream(baos), "utf-8")
      out.write("[")
      didOne = false
    }

    def maybeComma() {
      if(didOne) out.write(",")
      else didOne = true
    }

    def maybeFlush() {
      if(baos.size > 128000) flush()
    }

    def flush() {
      if(didOne) {
        out.write("]")
        out.close()
        val bytes = baos.toByteArray
        reset()
        auxUser(bytes)
      }
    }

    implicit val jCodec = new JsonCodec[PerfValue] {
      def encode(x: PerfValue) = x match {
        case PVId(i) => JArray(Seq(JNumber(i)))
        case PVText(s) => JString(s)
        case PVNumber(n) => JNumber(n)
        case PVNull => JNull
      }

      def decode(x: JValue) = x match {
        case JArray(Seq(JNumber(i))) => Some(PVId(i.longValue))
        case JString(s) => Some(PVText(s))
        case JNumber(n) => Some(PVNumber(n.toLong))
        case JNull => Some(PVNull)
        case _ => None
      }
    }

    val codec = implicitly[JsonCodec[Map[String, Map[String, PerfValue]]]]

    def insert(sid: Long, row: Row[PerfValue]) {
      maybeComma()
      // sorting because this is test code and we want to be able to use it sanely
      out.write(JsonUtil.renderJson(Map("i" -> (row + (datasetContext.systemIdColumnName -> PVId(sid)))))(codec))
      maybeFlush()
    }

    def update(sid: Long, row: Row[PerfValue]) {
      maybeComma()
      out.write(JsonUtil.renderJson(Map("u" -> (row + (datasetContext.systemIdColumnName -> PVId(sid)))))(codec))
      maybeFlush()
    }

    def delete(systemID: Long) {
      maybeComma()
      out.write(systemID.toString)
      maybeFlush()
    }

    def finish() {
      flush()
    }
  }

  def selectRow(id: PerfValue) = null

  def extract(resultSet: ResultSet, logicalColumn: String) = null

  // This may batch the "ids" into multiple queries.  The queries
  def findSystemIds(ids: Iterator[PerfValue]) = {
    require(datasetContext.hasUserPrimaryKey, "findSystemIds called without a user primary key")
    ids.grouped(100).map { block =>
      block.map(_.sqlize).mkString("SELECT id AS sid, " + pkCol + " AS uid FROM " + dataTableName + " WHERE " + pkCol + " IN (", ",", ")")
    }
  }

  def extractIdPairs(rs: ResultSet) = {
    val typ = datasetContext.userSchema(datasetContext.userPrimaryKeyColumn.getOrElse(sys.error("extractIdPairs called without a user primary key")))

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

    def loop(): Stream[IdPair[PerfValue]] = {
      if(rs.next()) {
        val sid = rs.getLong("sid")
        val uid = extractor()
        IdPair(sid, uid) #:: loop()
      } else {
        Stream.empty
      }
    }
    loop().iterator
  }
}
