package com.socrata.datacoordinator
package truth.loader
package sql

import com.socrata.datacoordinator._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Tag}

import scala.collection.immutable.VectorBuilder
import java.sql.{Connection, DriverManager, ResultSet}

import org.scalatest.MustMatchers
import org.scalatest.prop.PropertyChecks
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.util.{NoopTimingReport, RowDataProvider, RowIdProvider, RowVersionProvider}
import com.socrata.datacoordinator.id.{ColumnId, RowId, RowVersion}
import com.socrata.datacoordinator.util.collection.{ColumnIdMap, MutableColumnIdMap}
import com.rojoma.json.v3.ast.{JNumber, JString, JValue}
import org.scalacheck.{Arbitrary, Gen}

class TestSqlLoader extends FunSuite with MustMatchers with PropertyChecks with BeforeAndAfterAll {

  Class.forName("org.h2.Driver") // force driver to load

  val executor = java.util.concurrent.Executors.newCachedThreadPool()

  override def afterAll() {
    executor.shutdownNow()
  }

  def idProvider(initial: Long) = new RowIdProvider(new RowDataProvider(initial))
  def versionProvider(initial: Long) = new RowVersionProvider(new RowDataProvider(initial))

  def withDB[T]()(f: Connection => T): T = {
    using(DriverManager.getConnection("jdbc:h2:mem:")) { conn =>
      conn.setAutoCommit(false)
      f(conn)
    }
  }

  def execute(conn: Connection, sql: String) {
    using(conn.createStatement()) { stmt =>
      stmt.execute(sql)
    }
  }

  def query(conn: Connection, sql: String): Seq[Map[String, Any]] =
    for {
      stmt <- managed(conn.createStatement())
      rs <- managed(stmt.executeQuery(sql))
    } yield {
      val result = new VectorBuilder[Map[String, Any]]
      while(rs.next()) {
        result += (1 to rs.getMetaData.getColumnCount).foldLeft(Map.empty[String, Any]) { (row, col) =>
          row + (rs.getMetaData.getColumnLabel(col).toLowerCase -> rs.getObject(col))
        }
      }
      result.result()
    }

  val idCol: ColumnId = new ColumnId(0)
  val idColName = "c_" + idCol.underlying
  val versionCol: ColumnId = new ColumnId(1)
  val versionColName = "c_" + versionCol.underlying

  def makeTables(conn: Connection, ctx: AbstractRepBasedDataSqlizer[TestColumnType, TestColumnValue], logTableName: String) {
    execute(conn, "drop table if exists " + ctx.dataTableName)
    execute(conn, "drop table if exists " + logTableName)
    execute(conn, "CREATE TABLE " + ctx.dataTableName + " (c_" + idCol.underlying + " bigint not null primary key," + ctx.datasetContext.schema.iterator.filter(_._1 != idCol).map { case (c,t) =>
      val sqltype = t.representedType match {
        case LongColumn => "BIGINT"
        case StringColumn => "VARCHAR(100)"
      }
      "c_" + c.underlying + " " + sqltype + (if(ctx.datasetContext.userPrimaryKeyColumn == Some(c)) " NOT NULL" else " NULL")
    }.mkString(",") + ")")
    ctx.datasetContext.userPrimaryKeyColumn.foreach { pkCol =>
      execute(conn, "CREATE INDEX " + ctx.dataTableName + "_userid ON " + ctx.dataTableName + "(c_" + pkCol.underlying + ")")
    }
    // varchar rows because h2 returns a clob for TEXT columns instead of a string
    execute(conn, "CREATE TABLE " + logTableName + " (version bigint not null, subversion bigint not null, rows varchar(65536) not null, who varchar(100) null, PRIMARY KEY(version, subversion))")
  }

  def preload(conn: Connection, ctx: AbstractRepBasedDataSqlizer[TestColumnType, TestColumnValue], logTableName: String)(rows: Row[TestColumnValue]*) {
    makeTables(conn, ctx, logTableName)
    for(row <- rows) {
      val LongValue(id) = row.getOrElse(ctx.datasetContext.systemIdColumn, sys.error("No :id"))
      val remainingColumns = new MutableRow[TestColumnValue](row)
      remainingColumns -= ctx.datasetContext.systemIdColumn
      assert(remainingColumns.keySet.toSet.subsetOf(ctx.datasetContext.userColumnIds.toSet), "row contains extraneous keys")
      val sql = "insert into " + ctx.dataTableName + " (" + idColName + "," + remainingColumns.keys.map { c => "c_" + c.underlying }.mkString(",") + ") values (" + id + "," + remainingColumns.values.map(_.sqlize).mkString(",") + ")"
      execute(conn, sql)
    }
  }

  val standardTableBase = "test"
  val standardTableName = standardTableBase + "_data"
  val standardLogTableName = standardTableBase + "_log"
  val num = new ColumnId(10L)
  val numName = "c_" + num.underlying
  val str = new ColumnId(11L)
  val strName = "c_" + str.underlying
  val standardSchema = ColumnIdMap(idCol -> new LongRep(idCol), versionCol -> new LongRep(versionCol), num -> new LongRep(num), str -> new StringRep(str))

  val rawSelect = "SELECT " + idColName + ", " + versionColName + ", " + numName + ", " + strName + " from test_data"

  def rowPreparer(pkCol: ColumnId) = new RowPreparer[TestColumnValue] {
    def prepareForInsert(row: Row[TestColumnValue], sid: RowId, version: RowVersion) = {
      val newRow = new MutableColumnIdMap(row)
      newRow(idCol) = LongValue(sid.underlying)
      newRow(versionCol) = LongValue(version.underlying)
      newRow.freeze()
    }

    def prepareForUpdate(row: Row[TestColumnValue], oldRow: Row[TestColumnValue], newVersion: RowVersion) = {
      val m = new MutableRow(oldRow)
      m ++= row
      m(versionCol) = LongValue(newVersion.underlying)
      m.freeze()
    }
  }

  def simpleReportWriter() = new SimpleReportWriter[TestColumnValue]

  test("building a report before finished fails (simple report writer)") {
    val e = the [AssertionError] thrownBy { simpleReportWriter().report }
    e.getMessage must include ("report() called without being finished first")
  }

  def testOps(name: String, testTags: Tag*)(testWithBySystemId: Boolean => Unit): Unit = {
    def testOn(bySystemId: Boolean) = test(s"$name when by_system_id is $bySystemId", testTags: _*) {
      testWithBySystemId(bySystemId)
    }
    testOn(bySystemId = false)
    testOn(bySystemId = true)
  }

  testOps("adding a new row with system PK succeeds") { bySystemId =>
    val dsContext = new TestDatasetContext(standardSchema, idCol, None, versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), false, dataSqlizer, dataLogger, idProvider(15), versionProvider(53), executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row[TestColumnValue](num -> LongValue(1), str -> StringValue("a")), bySystemId = bySystemId)
        txn.finish()
        dataLogger.finish()

        val report = reportWriter.report

        report.inserted must equal (Map(0 -> IdAndVersion(LongValue(15), new RowVersion(53), bySystemIdForced = false))) // inserts are never "by_system_id"
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be ('empty)
      }
      conn.commit()

      query(conn, rawSelect) must equal (Seq(
        Map(idColName -> 15L, versionColName -> 53L, numName -> 1L, strName -> "a")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq(
        Map("version" -> 1L, "subversion" -> 1L, "rows" -> ("""[{"i":""" + logMap(idCol -> JNumber(15), versionCol -> JNumber(53), num -> JNumber(1), str -> JString("a")) + """}]"""), "who" -> "hello")
      ))
    }
  }

  def logMap(cols: (ColumnId, JValue)*) = {
    val sb = new StringBuilder("{")
    var didOne = false
    for((k,v) <- cols.sortBy(_._1)) {
      if(didOne) sb.append(',')
      else didOne = true
      sb.append('"').append(k.underlying).append("\":").append(v)
    }
    sb.append('}').toString
  }

  testOps("inserting and then updating a new row with system PK succeeds") { bySystemId =>
    val ids = idProvider(15)
    val vers = versionProvider(431)
    val dsContext = new TestDatasetContext(standardSchema, idCol, None, versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row[TestColumnValue](num -> LongValue(1), str -> StringValue("a")), bySystemId = bySystemId)
        txn.upsert(1, Row[TestColumnValue](idCol -> LongValue(15), num -> LongValue(2), str -> StringValue("b")), bySystemId = bySystemId)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must equal (Map(0 -> IdAndVersion(LongValue(15), new RowVersion(431), bySystemIdForced = false))) // inserts are never "by_system_id"
        report.updated must be (Map(1 -> IdAndVersion(LongValue(15), new RowVersion(432), bySystemIdForced = false))) // inserts are never "by_system_id" when the primary key is the system id
        report.deleted must be ('empty)
        report.errors must be ('empty)
      }
      conn.commit()

      ids.underlying.finish() must be (16L)
      vers.underlying.finish() must be (433L)

      query(conn, rawSelect) must equal (Seq(Map(idColName -> 15L, versionColName -> 432L, numName -> 2L, strName -> "b")))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq(
        locally {
          val op1 = logMap(idCol -> JNumber(15), versionCol -> JNumber(431), num -> JNumber(1), str -> JString("a"))
          val op2 = logMap(versionCol -> JNumber(432), num -> JNumber(2), str -> JString("b"))
          Map("version" -> 1L, "subversion" -> 1L, "rows" -> ("""[{"i":""" + op1 + """},{"u":[""" + op1 + "," + op2 + """]}]"""), "who" -> "hello")
        }
      ))
    }
  }

  testOps("trying to add a new row with a NULL system PK does not fail (it adds a row with a generated ID)") { bySystemId =>
    val dsContext = new TestDatasetContext(standardSchema, idCol, None, versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      val ids = idProvider(22)
      val vers = versionProvider(111)

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(idCol -> NullValue, num -> LongValue(1), str -> StringValue("a")), bySystemId = bySystemId)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be (Map(0 -> IdAndVersion(LongValue(22), new RowVersion(111), bySystemIdForced = false))) // inserts are never "by_system_id"
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be ('empty)
      }
      conn.commit()

      ids.underlying.finish() must be (23L)
      vers.underlying.finish() must be (112L)

      query(conn, rawSelect) must equal (Seq(Map(idColName -> 22L, versionColName -> 111L, numName -> 1L, strName -> "a")))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq(
        locally {
          val op = logMap(idCol -> JNumber(22), versionCol -> JNumber(111), num -> JNumber(1), str -> JString("a"))
          Map("version" -> 1L, "subversion" -> 1L, "rows" -> ("""[{"i":""" + op + """}]"""), "who" -> "hello")
        }
      ))
    }
  }

  testOps("trying to add a new row with a nonexistant system PK fails") { bySystemId =>
    val dsContext = new TestDatasetContext(standardSchema, idCol, None, versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      val ids = idProvider(6)
      val vers = versionProvider(99)

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(idCol -> LongValue(77), num -> LongValue(1), str -> StringValue("a")), bySystemId = bySystemId)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must equal (Map(0 -> NoSuchRowToUpdate(LongValue(77), bySystemIdForced = false))) // updates are never "by_system_id" when the primary key is the system id
      }
      conn.commit()

      ids.underlying.finish() must be (6L)
      vers.underlying.finish() must be (99L)

      query(conn, rawSelect) must equal (Seq.empty)
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq.empty)
    }
  }

  testOps("updating an existing row by system id succeeds") { bySystemId =>
    val ids = idProvider(13)
    val vers = versionProvider(5)
    val dsContext = new TestDatasetContext(standardSchema, idCol, None, versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      preload(conn, dataSqlizer, standardLogTableName)(
        Row(idCol -> LongValue(7), versionCol -> LongValue(99), str -> StringValue("q"), num -> LongValue(2))
      )
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(idCol -> LongValue(7), num -> LongValue(44)), bySystemId = bySystemId)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must equal (Map(0 -> IdAndVersion(LongValue(7), new RowVersion(5), bySystemIdForced = false))) // inserts are never "by_system_id" when the primary key is the system id
        report.deleted must be ('empty)
        report.errors must be ('empty)
      }
      conn.commit()

      ids.underlying.finish() must be (13L)
      vers.underlying.finish() must be (6L)

      query(conn, rawSelect) must equal (Seq(
        Map(idColName -> 7L, versionColName -> 5L, numName -> 44L, strName -> "q")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq(
        locally {
          val oldData = logMap(idCol -> JNumber(7), versionCol -> JNumber(99), num -> JNumber(2), str -> JString("q"))
          val op = logMap(versionCol -> JNumber(5), num -> JNumber(44))
          Map("version" -> 1L, "subversion" -> 1L, "rows" -> ("""[{"u":[""" + oldData + "," + op + """]}]"""), "who" -> "hello")
        }
      ))
    }
  }

  testOps("adding a new row with user PK succeeds") { bySystemId =>
    val ids = idProvider(15)
    val vers = versionProvider(999999)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(str), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(num -> LongValue(1), str -> StringValue("a")), bySystemId = bySystemId)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must equal (Map(0 -> IdAndVersion(StringValue("a"), new RowVersion(999999), bySystemIdForced = false))) // inserts are never "by_system_id"
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be ('empty)
      }
      conn.commit()

      ids.underlying.finish() must equal (16L)
      vers.underlying.finish() must equal (1000000L)

      query(conn, rawSelect) must equal (Seq(
        Map(idColName -> 15L, versionColName -> 999999L, numName -> 1L, strName -> "a")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq(
        locally {
          val op = logMap(idCol -> JNumber(15), versionCol -> JNumber(999999), num -> JNumber(1), str -> JString("a"))
          Map("version" -> 1L, "subversion" -> 1L, "rows" -> ("""[{"i":""" + op + """}]"""), "who" -> "hello")
        }
      ))
    }
  }

  testOps("trying to add a new row with a NULL user PK fails") { bySystemId =>
    val ids = idProvider(22)
    val vers = versionProvider(8)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(str), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(num -> LongValue(1), str -> NullValue), bySystemId = bySystemId)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must equal (Map(0 -> NoPrimaryKey))
      }
      conn.commit()

      ids.underlying.finish() must equal (22L)
      vers.underlying.finish() must equal (8L)

      query(conn, rawSelect) must equal (Seq.empty)
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq.empty)
    }
  }

  testOps("trying to add a new row without user PK fails (and no system id)") { bySystemId =>
    val ids = idProvider(22)
    val vers = versionProvider(16)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(str), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(num -> LongValue(1)), bySystemId = bySystemId)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be (Map(0 -> NoPrimaryKey))
      }
      conn.commit()

      ids.underlying.finish() must equal (22L)
      vers.underlying.finish() must equal (16L)

      query(conn, rawSelect) must equal (Seq.empty)
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq.empty)
    }
  }

  testOps("updating an existing row by user pk succeeds") { bySystemId =>
    val ids = idProvider(13)
    val vers = versionProvider(24)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      preload(conn, dataSqlizer, standardLogTableName)(
        Row(idCol -> LongValue(7), versionCol -> LongValue(11001001), str -> StringValue("q"), num -> LongValue(2))
      )
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(str), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(str -> StringValue("q"), num -> LongValue(44)), bySystemId = bySystemId)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must equal (Map(0 -> IdAndVersion(StringValue("q"), new RowVersion(24), bySystemIdForced = false)))
        report.deleted must be ('empty)
        report.errors must be ('empty)
      }
      conn.commit()

      ids.underlying.finish() must be (13L)
      vers.underlying.finish() must be (25L)

      query(conn, rawSelect) must equal (Seq(
        Map(idColName -> 7L, versionColName -> 24L, numName -> 44L, strName -> "q")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq(
        locally {
          val oldData = logMap(idCol -> JNumber(7), versionCol -> JNumber(11001001), num -> JNumber(2), str -> JString("q"))
          val op = logMap(versionCol -> JNumber(24), num -> JNumber(44))
          Map("version" -> 1L, "subversion" -> 1L, "rows" -> ("""[{"u":[""" + oldData + "," + op + """]}]"""), "who" -> "hello")
        }
      ))
    }
  }

  testOps("inserting and then updating a new row with user PK succeeds") { bySystemId =>
    val ids = idProvider(15)
    val vers = versionProvider(32)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(str), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(num -> LongValue(1), str -> StringValue("q")), bySystemId = bySystemId)
        txn.upsert(1, Row(num -> LongValue(2), str -> StringValue("q")), bySystemId = bySystemId)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must equal (Map(0 -> IdAndVersion(StringValue("q"), new RowVersion(32), bySystemIdForced = false)))
        report.updated must be (Map(1 -> IdAndVersion(StringValue("q"), new RowVersion(33), bySystemIdForced = false)))
        report.deleted must be ('empty)
        report.errors must be ('empty)
      }
      conn.commit()

      ids.underlying.finish() must be (16L)
      vers.underlying.finish() must be (34L)

      query(conn, rawSelect) must equal (Seq(
        Map(idColName -> 15L, versionColName -> 33L, numName -> 2L, strName -> "q")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq(
        locally {
          val op1 = logMap(idCol -> JNumber(15), versionCol -> JNumber(32), num -> JNumber(1), str -> JString("q"))
          val op2 = logMap(versionCol -> JNumber(33), num -> JNumber(2))
          Map("version" -> 1L, "subversion" -> 1L, "rows" -> ("""[{"i":""" + op1 + """},{"u":[""" + op1 + "," + op2 + """]}]"""), "who" -> "hello")
        }
      ))
    }
  }

  testOps("specifying :id when there's a user PK succeeds (and ignores it)") { bySystemId =>
    val ids = idProvider(15)
    val vers = versionProvider(40)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(str), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(idCol -> LongValue(15), num -> LongValue(1), str -> StringValue("q")), bySystemId = bySystemId)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be (Map(0 -> IdAndVersion(StringValue("q"), new RowVersion(40), bySystemIdForced = false))) // inserts are never "by_system_id"
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be ('empty)
      }
      conn.commit()

      ids.underlying.finish() must be (16L)
      vers.underlying.finish() must be (41L)

      query(conn, rawSelect) must equal (Seq(
        Map(idColName -> 15L, versionColName -> 40L, numName -> 1L, strName -> "q")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq(
        locally {
          val op = logMap(idCol -> JNumber(15), versionCol -> JNumber(40), num -> JNumber(1), str -> JString("q"))
          Map("version" -> 1L, "subversion" -> 1L, "rows" -> ("""[{"i":""" + op + """}]"""), "who" -> "hello")
        }
      ))
    }
  }

  // tests for where by_system_id changes behavior of upsert:
  test("specifying :id for insert when the user PK is not passed fails when by_system_id is false") {
    val ids = idProvider(15)
    val vers = versionProvider(40)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(str), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(idCol -> LongValue(7), num -> LongValue(2)), bySystemId = false)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be (Map(0 -> NoPrimaryKey))
      }
      conn.commit()

      // we did not insert or update any rows
      ids.underlying.finish() must be (15L)
      vers.underlying.finish() must be (40L)

      // the preloaded row
      query(conn, rawSelect) must equal (Seq.empty)
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq.empty)
    }
  }

  test("specifying :id for insert when the user PK is not passed fails when by_system_id is true") {
    val ids = idProvider(15)
    val vers = versionProvider(40)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(str), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(idCol -> LongValue(7), num -> LongValue(2)), bySystemId = true)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be (Map(0 -> NoSuchRowToUpdate(LongValue(7), bySystemIdForced = true)))
      }
      conn.commit()

      // we did not insert or update any rows
      ids.underlying.finish() must be (15L)
      vers.underlying.finish() must be (40L)

      // the preloaded row
      query(conn, rawSelect) must equal (Seq.empty)
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq.empty)
    }
  }

  test("specifying :id when the user PK is not passed fails when by_system_id is false") {
    val ids = idProvider(15)
    val vers = versionProvider(40)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)

      preload(conn, dataSqlizer, standardLogTableName)(
        Row(idCol -> LongValue(7), versionCol -> LongValue(33), str -> StringValue("q"), num -> LongValue(1))
      )
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(str), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(idCol -> LongValue(7), num -> LongValue(2)), bySystemId = false)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be (Map(0 -> NoPrimaryKey))
      }
      conn.commit()

      // we did not insert or update any rows
      ids.underlying.finish() must be (15L)
      vers.underlying.finish() must be (40L)

      // the preloaded row
      query(conn, rawSelect) must equal (Seq(
        Map(idColName -> 7L, versionColName -> 33L, numName -> 1L, strName -> "q")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq.empty)
    }
  }

  test("specifying :id when the user PK is not passed succeeds when by_system_id is true") {
    val ids = idProvider(15)
    val vers = versionProvider(40)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)

      preload(conn, dataSqlizer, standardLogTableName)(
        Row(idCol -> LongValue(7), versionCol -> LongValue(33), str -> StringValue("q"), num -> LongValue(1))
      )
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(str), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(idCol -> LongValue(7), num -> LongValue(2)), bySystemId = true)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be (Map(0 -> IdAndVersion(LongValue(7), new RowVersion(40), bySystemIdForced = true)))
        report.deleted must be ('empty)
        report.errors must be ('empty)
      }
      conn.commit()

      // updated 1 row
      ids.underlying.finish() must be (15L)
      vers.underlying.finish() must be (41L)

      query(conn, rawSelect) must equal (Seq(
        Map(idColName -> 7L, versionColName -> 40L, numName -> 2L, strName -> "q")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must be(Seq(
        locally {
          val oldData = logMap(idCol -> JNumber(7), versionCol -> JNumber(33), num -> JNumber(1), str -> JString("q"))
          val op = logMap(versionCol -> JNumber(40), num -> JNumber(2))
          Map("version" -> 1L, "subversion" -> 1L, "rows" -> ("""[{"u":[""" + oldData + "," + op +  """]}]"""), "who" -> "hello")
        }
      ))
    }
  }

  test("using both :id and the user PK succeeds when by_system_id is true") {
    val ids = idProvider(15)
    val vers = versionProvider(40)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)

      preload(conn, dataSqlizer, standardLogTableName)(
        Row(idCol -> LongValue(7), versionCol -> LongValue(33), str -> StringValue("q"), num -> LongValue(1)),
        Row(idCol -> LongValue(8), versionCol -> LongValue(34), str -> StringValue("r"), num -> LongValue(2)),
        Row(idCol -> LongValue(9), versionCol -> LongValue(35), str -> StringValue("s"), num -> LongValue(3)),
        Row(idCol -> LongValue(10), versionCol -> LongValue(36), str -> StringValue("t"), num -> LongValue(4))
      )
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(str), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(str -> StringValue("u"), num -> LongValue(5)), bySystemId = true)
        txn.upsert(1, Row(idCol -> LongValue(7), num -> LongValue(2)), bySystemId = true)
        txn.upsert(2, Row(idCol -> LongValue(8), num -> LongValue(4)), bySystemId = true)
        txn.upsert(3, Row(str -> StringValue("v"), num -> LongValue(6)), bySystemId = true)
        txn.upsert(4, Row(str -> StringValue("s"), num -> LongValue(6)), bySystemId = true)
        txn.upsert(5, Row(str -> StringValue("t"), num -> LongValue(8)), bySystemId = true)
        txn.upsert(6, Row(idCol -> LongValue(9), num -> LongValue(9)), bySystemId = true)
        txn.upsert(7, Row(idCol -> LongValue(10), num -> LongValue(12)), bySystemId = true)
        txn.upsert(8, Row(idCol -> LongValue(11), num -> LongValue(0)), bySystemId = true)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        // inserts are never "by_system_id"
        report.inserted must be (Map(
          0 -> IdAndVersion(StringValue("u"), new RowVersion(40), bySystemIdForced = false),
          3 -> IdAndVersion(StringValue("v"), new RowVersion(43), bySystemIdForced = false)))
        report.updated must be (Map(
          1 -> IdAndVersion(LongValue(7), new RowVersion(41), bySystemIdForced = true),
          2 -> IdAndVersion(LongValue(8), new RowVersion(42), bySystemIdForced = true),
          4 -> IdAndVersion(StringValue("s"), new RowVersion(44), bySystemIdForced = false),
          5 -> IdAndVersion(StringValue("t"), new RowVersion(45), bySystemIdForced = false),
          6 -> IdAndVersion(LongValue(9), new RowVersion(46), bySystemIdForced = true),
          7 -> IdAndVersion(LongValue(10), new RowVersion(47), bySystemIdForced = true)))
        report.deleted must be ('empty)
        report.errors must be (Map(8 -> NoSuchRowToUpdate(LongValue(11), bySystemIdForced = true)))
      }
      conn.commit()

      // inserted 2 rows and updated rows 6 times
      ids.underlying.finish() must be (17L)
      vers.underlying.finish() must be (48L)

      query(conn, rawSelect) must equal (Seq(
        Map(idColName -> 7L, versionColName -> 41L, numName -> 2L, strName -> "q"),
        Map(idColName -> 8L, versionColName -> 42L, numName -> 4L, strName -> "r"),
        Map(idColName -> 9L, versionColName -> 46L, numName -> 9L, strName -> "s"),
        Map(idColName -> 10L, versionColName -> 47L, numName -> 12L, strName -> "t"),
        Map(idColName -> 15L, versionColName -> 40L, numName -> 5L, strName -> "u"),
        Map(idColName -> 16L, versionColName -> 43L, numName -> 6L, strName -> "v")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must be(Seq(
        locally {
          val op0 = logMap(idCol -> JNumber(15), versionCol -> JNumber(40), num -> JNumber(5), str -> JString("u"))

          val old1 = logMap(idCol -> JNumber(7), versionCol -> JNumber(33), num -> JNumber(1), str -> JString("q"))
          val op1 = logMap(versionCol -> JNumber(41), num -> JNumber(2))

          val old2 = logMap(idCol -> JNumber(8), versionCol -> JNumber(34), num -> JNumber(2), str -> JString("r"))
          val op2 = logMap(versionCol -> JNumber(42), num -> JNumber(4))

          val op3 = logMap(idCol -> JNumber(16), versionCol -> JNumber(43), num -> JNumber(6), str -> JString("v"))

          val old4 = logMap(idCol -> JNumber(9), versionCol -> JNumber(35), num -> JNumber(3), str -> JString("s"))
          val op4 = logMap(versionCol -> JNumber(44), num -> JNumber(6))

          val old5 = logMap(idCol -> JNumber(10), versionCol -> JNumber(36), num -> JNumber(4), str -> JString("t"))
          val op5 = logMap(versionCol -> JNumber(45), num -> JNumber(8))

          val old6 = logMap(idCol -> JNumber(9), versionCol -> JNumber(44), num -> JNumber(6), str -> JString("s"))
          val op6 = logMap(versionCol -> JNumber(46), num -> JNumber(9))

          val old7 = logMap(idCol -> JNumber(10), versionCol -> JNumber(45), num -> JNumber(8), str -> JString("t"))
          val op7 = logMap(versionCol -> JNumber(47), num -> JNumber(12))

          val rows: String = (
            """[
              |{"i":""".stripMargin + op0 + """},
              |{"u":[""".stripMargin + old1 + "," + op1 + """]},
              |{"u":[""".stripMargin + old2 + "," + op2 + """]},
              |{"i":""".stripMargin + op3 + """},
              |{"u":[""".stripMargin + old4 + "," + op4 + """]},
              |{"u":[""".stripMargin + old5 + "," + op5 + """]},
              |{"u":[""".stripMargin + old6 + "," + op6 + """]},
              |{"u":[""".stripMargin + old7 + "," + op7 + """]}
              |]""".stripMargin).replaceAllLiterally("\n", "")

          Map("version" -> 1L, "subversion" -> 1L,"rows" -> rows, "who" -> "hello")
        }
      ))
    }
  }

  test("deleting a row just inserted with a user PK succeeds when by_system_id is false") {
    val ids = idProvider(15)
    val vers = versionProvider(48)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(str), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(num -> LongValue(1), str -> StringValue("q")), bySystemId = false)
        txn.delete(1, StringValue("q"), None, bySystemId = false)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be (Map(0 -> IdAndVersion(StringValue("q"), new RowVersion(48), bySystemIdForced = false)))
        report.updated must be ('empty)
        report.deleted must equal (Map(1 -> StringValue("q")))
        report.errors must be ('empty)
      }
      conn.commit()

      ids.underlying.finish() must be (16L)
      vers.underlying.finish() must be (49L)

      query(conn, rawSelect) must equal (Seq.empty)
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq(
        locally {
          val op = logMap(idCol -> JNumber(15), versionCol -> JNumber(48), num -> JNumber(1), str -> JString("q"))
          Map("version" -> 1L, "subversion" -> 1L, "rows" -> s"""[{"i":$op},{"d":$op}]""", "who" -> "hello")
        }
      ))
    }
  }

  test("deleting a row just inserted with a user PK fails when by_system_id is true") {
    val ids = idProvider(15)
    val vers = versionProvider(48)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(str), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        val err = intercept[java.lang.ClassCastException] {
          txn.upsert(0, Row(num -> LongValue(1), str -> StringValue("q")), bySystemId = false) // inserts are never "by_system_id"
          txn.delete(1, StringValue("q"), None, bySystemId = true)
          txn.finish()
        }
        err.getMessage must be("com.socrata.datacoordinator.truth.loader.sql.StringValue cannot be cast to com.socrata.datacoordinator.truth.loader.sql.LongValue")
      }
    }
  }

  testOps("deleting a row just inserted with a system PK succeeds") { bySystemId =>
    // This isn't a useful thing to be able to do, since in the real system
    // IDs won't be user-predictable, but it's a valuable sanity check anyway.
    val ids = idProvider(15)
    val vers = versionProvider(56)
    val dsContext = new TestDatasetContext(standardSchema, idCol, None, versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(num -> LongValue(1), str -> StringValue("q")), bySystemId = bySystemId)
        txn.delete(1, LongValue(15), None, bySystemId = bySystemId)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be (Map(0 -> IdAndVersion(LongValue(15), new RowVersion(56), bySystemIdForced = false))) // inserts are never "by_system_id"
        report.updated must be ('empty)
        report.deleted must equal (Map(1 -> LongValue(15)))
        report.errors must be ('empty)
      }
      conn.commit()

      ids.underlying.finish() must be (16L)
      vers.underlying.finish() must be (57L)

      query(conn, rawSelect) must equal (Seq.empty)
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq(
        locally {
          val op = logMap(idCol -> JNumber(15), versionCol -> JNumber(56), num -> JNumber(1), str -> JString("q"))
          Map("version" -> 1L, "subversion" -> 1L, "rows" -> s"""[{"i":$op},{"d":$op}]""", "who" -> "hello")
        }
      ))
    }
  }

  test("deleting a row by :id when there is a user primary key fails when by_system_id is false") {
    val ids = idProvider(15)
    val vers = versionProvider(48)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)

      preload(conn, dataSqlizer, standardLogTableName)(
        Row(idCol -> LongValue(7), versionCol -> LongValue(33), str -> StringValue("q"), num -> LongValue(1))
      )
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(str), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        val err = intercept[java.lang.ClassCastException] {
          txn.delete(0, LongValue(7), None, bySystemId = false)
          txn.finish()
        }
        err.getMessage must be("com.socrata.datacoordinator.truth.loader.sql.LongValue cannot be cast to com.socrata.datacoordinator.truth.loader.sql.StringValue")
      }
    }
  }

  test("deleting a row by :id when there is a user primary key succeeds when by_system_id is true") {
    val ids = idProvider(15)
    val vers = versionProvider(48)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)

      preload(conn, dataSqlizer, standardLogTableName)(
        Row(idCol -> LongValue(7), versionCol -> LongValue(33), str -> StringValue("q"), num -> LongValue(1))
      )
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(str), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.delete(0, LongValue(7), None, bySystemId = true)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must equal (Map(0 -> LongValue(7)))
        report.errors must be ('empty)
      }
      conn.commit()

      ids.underlying.finish() must be (15L)
      vers.underlying.finish() must be (48L)

      query(conn, rawSelect) must equal (Seq.empty)
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq(
        locally {
          val op = logMap(idCol -> JNumber(7), versionCol -> JNumber(33), num -> JNumber(1), str -> JString("q"))
          Map("version" -> 1L, "subversion" -> 1L, "rows" -> s"""[{"d":$op}]""", "who" -> "hello")
        }
      ))
    }
  }

  test("inserting a row with an explicitly null version succeeds (system PK)") {
    val ids = idProvider(15)
    val vers = versionProvider(64)
    val dsContext = new TestDatasetContext(standardSchema, idCol, None, versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(num -> LongValue(1), str -> StringValue("q"), versionCol -> NullValue), bySystemId = false)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must equal (Map(0 -> IdAndVersion(LongValue(15), new RowVersion(64), bySystemIdForced = false)))
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be ('empty)
      }
      conn.commit()

      ids.underlying.finish() must be (16L)
      vers.underlying.finish() must be (65L)

      query(conn, rawSelect) must equal (Seq(
        Map(idColName -> 15L, versionColName -> 64L, numName -> 1L, strName -> "q")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq(
        locally {
          val op = logMap(idCol -> JNumber(15), versionCol -> JNumber(64), num -> JNumber(1), str -> JString("q"))
          Map("version" -> 1L, "subversion" -> 1L, "rows" -> ("""[{"i":""" + op + """}]"""), "who" -> "hello")
        }
      ))
    }
  }

  test("inserting a row with an explicitly null version succeeds (user PK)") {
    val ids = idProvider(15)
    val vers = versionProvider(72)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(num -> LongValue(1), str -> StringValue("q"), versionCol -> NullValue), bySystemId = false)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must equal (Map(0 -> IdAndVersion(StringValue("q"), new RowVersion(72), bySystemIdForced = false)))
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be ('empty)
      }
      conn.commit()

      ids.underlying.finish() must be (16L)
      vers.underlying.finish() must be (73L)

      query(conn, rawSelect) must equal (Seq(
        Map(idColName -> 15L, versionColName -> 72L, numName -> 1L, strName -> "q")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq(
        locally {
          val op = logMap(idCol -> JNumber(15), versionCol -> JNumber(72), num -> JNumber(1), str -> JString("q"))
          Map("version" -> 1L, "subversion" -> 1L, "rows" -> ("""[{"i":""" + op + """}]"""), "who" -> "hello")
        }
      ))
    }
  }

  test("inserting a row with a not-null version fails (system PK)") {
    val ids = idProvider(15)
    val vers = versionProvider(80)
    val dsContext = new TestDatasetContext(standardSchema, idCol, None, versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(num -> LongValue(1), str -> StringValue("q"), versionCol -> LongValue(0)), bySystemId = false)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be (Map(0 -> VersionOnNewRow))
      }
      conn.commit()

      ids.underlying.finish() must be (15L)
      vers.underlying.finish() must be (80L)

      query(conn, rawSelect) must equal (Seq.empty)
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq.empty)
    }
  }

  test("inserting a row with a not-null version fails (user PK)") {
    val ids = idProvider(15)
    val vers = versionProvider(88)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(num -> LongValue(1), str -> StringValue("q"), versionCol -> LongValue(0)), bySystemId = false)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be (Map(0 -> VersionMismatch(StringValue("q"), None, Some(new RowVersion(0)), bySystemIdForced = false)))
      }
      conn.commit()

      ids.underlying.finish() must be (15L)
      vers.underlying.finish() must be (88L)

      query(conn, rawSelect) must equal (Seq.empty)
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq.empty)
    }
  }

  test("updating a row with an explicitly null version fails (system PK)") {
    val ids = idProvider(15)
    val vers = versionProvider(96)
    val dsContext = new TestDatasetContext(standardSchema, idCol, None, versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      preload(conn, dataSqlizer, standardLogTableName)(
        Row(idCol -> LongValue(1), versionCol -> LongValue(0), str -> StringValue("q"), num -> LongValue(2))
      )
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(idCol -> LongValue(1), num -> LongValue(1), str -> StringValue("q"), versionCol -> NullValue), bySystemId = false)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be (Map(0 -> VersionMismatch(LongValue(1), Some(new RowVersion(0)), None, bySystemIdForced = false)))
      }
      conn.commit()

      ids.underlying.finish() must be (15L)
      vers.underlying.finish() must be (96L)

      query(conn, rawSelect) must equal (Seq(
        Map(idColName -> 1L, versionColName -> 0L, numName -> 2L, strName -> "q")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq.empty)
    }
  }

  test("updating a row with an explicitly null version fails (user PK)") {
    val ids = idProvider(15)
    val vers = versionProvider(104)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      preload(conn, dataSqlizer, standardLogTableName)(
        Row(idCol -> LongValue(1), versionCol -> LongValue(0), str -> StringValue("q"), num -> LongValue(2))
      )
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(num -> LongValue(1), str -> StringValue("q"), versionCol -> NullValue), bySystemId = false)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be (Map(0 -> VersionMismatch(StringValue("q"), Some(new RowVersion(0)), None, bySystemIdForced = false)))
      }
      conn.commit()

      ids.underlying.finish() must be (15L)
      vers.underlying.finish() must be (104L)

      query(conn, rawSelect) must equal (Seq(
        Map(idColName -> 1L, versionColName -> 0L, numName -> 2L, strName -> "q")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq.empty)
    }
  }

  test("updating a row with an incorrect not-null version fails (system PK)") {
    val ids = idProvider(15)
    val vers = versionProvider(112)
    val dsContext = new TestDatasetContext(standardSchema, idCol, None, versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      preload(conn, dataSqlizer, standardLogTableName)(
        Row(idCol -> LongValue(1), versionCol -> LongValue(0), str -> StringValue("q"), num -> LongValue(2))
      )
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(idCol -> LongValue(1), num -> LongValue(1), str -> StringValue("q"), versionCol -> LongValue(1)), bySystemId = false)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be (Map(0 -> VersionMismatch(LongValue(1), Some(new RowVersion(0)), Some(new RowVersion(1)), bySystemIdForced = false)))
      }
      conn.commit()

      ids.underlying.finish() must be (15L)
      vers.underlying.finish() must be (112L)

      query(conn, rawSelect) must equal (Seq(
        Map(idColName -> 1L, versionColName -> 0L, numName -> 2L, strName -> "q")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq.empty)
    }
  }

  test("updating a row with an incorrect not-null version fails (user PK)") {
    val ids = idProvider(15)
    val vers = versionProvider(120)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      preload(conn, dataSqlizer, standardLogTableName)(
        Row(idCol -> LongValue(1), versionCol -> LongValue(0), str -> StringValue("q"), num -> LongValue(2))
      )
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(num -> LongValue(1), str -> StringValue("q"), versionCol -> LongValue(1)), bySystemId = false)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be (Map(0 -> VersionMismatch(StringValue("q"), Some(new RowVersion(0)), Some(new RowVersion(1)), bySystemIdForced = false)))
      }
      conn.commit()

      ids.underlying.finish() must be (15L)
      vers.underlying.finish() must be (120L)

      query(conn, rawSelect) must equal (Seq(
        Map(idColName -> 1L, versionColName -> 0L, numName -> 2L, strName -> "q")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq.empty)
    }
  }

  test("updating a row with a correct not-null version succeeds (system PK)") {
    val ids = idProvider(15)
    val vers = versionProvider(128)
    val dsContext = new TestDatasetContext(standardSchema, idCol, None, versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      preload(conn, dataSqlizer, standardLogTableName)(
        Row(idCol -> LongValue(1), versionCol -> LongValue(0), str -> StringValue("q"), num -> LongValue(2))
      )
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(idCol -> LongValue(1), num -> LongValue(1), str -> StringValue("w"), versionCol -> LongValue(0)), bySystemId = false)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must equal (Map(0 -> IdAndVersion(LongValue(1), new RowVersion(128), bySystemIdForced = false)))
        report.deleted must be ('empty)
        report.errors must be ('empty)
      }
      conn.commit()

      ids.underlying.finish() must be (15L)
      vers.underlying.finish() must be (129L)

      query(conn, rawSelect) must equal (Seq(
        Map(idColName -> 1L, versionColName -> 128L, numName -> 1L, strName -> "w")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq(
        locally {
          val oldData = logMap(idCol -> JNumber(1), versionCol -> JNumber(0), str -> JString("q"), num -> JNumber(2))
          val op = logMap(versionCol -> JNumber(128), num -> JNumber(1), str -> JString("w"))
          Map("version" -> 1L, "subversion" -> 1L, "rows" -> ("""[{"u":[""" + oldData + "," + op + """]}]"""), "who" -> "hello")
        }
      ))
    }
  }

  test("updating a row with a correct not-null version succeeds (user PK)") {
    val ids = idProvider(15)
    val vers = versionProvider(136)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      preload(conn, dataSqlizer, standardLogTableName)(
        Row(idCol -> LongValue(1), versionCol -> LongValue(0), str -> StringValue("q"), num -> LongValue(2))
      )
      conn.commit()

      val reportWriter = simpleReportWriter()
      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName, idCol))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), false, dataSqlizer, dataLogger, ids, vers, executor, reportWriter, NoopTimingReport))
      } {
        txn.upsert(0, Row(num -> LongValue(1), str -> StringValue("q"), versionCol -> LongValue(0)), bySystemId = false)
        txn.finish()
        val report = reportWriter.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must equal (Map(0 -> IdAndVersion(StringValue("q"), new RowVersion(136), bySystemIdForced = false)))
        report.deleted must be ('empty)
        report.errors must be ('empty)
      }
      conn.commit()

      ids.underlying.finish() must be (15L)
      vers.underlying.finish() must be (137L)

      query(conn, rawSelect) must equal (Seq(
        Map(idColName -> 1L, versionColName -> 136L, numName -> 1L, strName -> "q")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq(
        locally {
          val oldData = logMap(idCol -> JNumber(1), versionCol -> JNumber(0), str -> JString("q"), num -> JNumber(2))
          val op = logMap(versionCol -> JNumber(136), num -> JNumber(1))
          Map("version" -> 1L, "subversion" -> 1L, "rows" -> ("""[{"u":[""" + oldData +"," + op + """]}]"""), "who" -> "hello")
        }
      ))
    }
  }

  sealed abstract class Op
  case class Upsert(id: Option[Long], num: Option[Long], data: Option[String]) extends Op
  case class Delete(id: Long) extends Op

  def testAgainstStupidSqlLoader[T](schema: ColumnIdMap[TestColumnRep],
                                    userIdCol: Option[ColumnId],
                                    genUpsert: Gen[Long] => Gen[Upsert],
                                    applyOps: (Loader[TestColumnValue], List[Op]) => Unit,
                                    query: String => String, // select query from t-table
                                    result: ResultSet => Vector[T]): Unit = {

    val genId = Gen.choose(0L, 100L)
    val genDelete = genId.map(Delete)

    val genOp = Gen.frequency(2 -> genUpsert(genId), 1 -> genDelete)

    implicit val arbOp = Arbitrary[Op](genOp)

    val dsContext = new TestDatasetContext(schema, idCol, userIdCol, versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    val stupidDsContext = new TestDatasetContext(schema, idCol, userIdCol, versionCol)
    val stupidDataSqlizer = new TestDataSqlizer("stupid_data", stupidDsContext)

    forAll { (opss: List[List[Op]]) =>
      withDB() { stupidConn =>
        withDB() { smartConn =>
          makeTables(smartConn, dataSqlizer, standardLogTableName)
          makeTables(stupidConn, stupidDataSqlizer, "stupid_log")

          val smartIds = idProvider(1)
          val stupidIds = idProvider(1)

          val smartVersions = versionProvider(1)
          val stupidVersions = versionProvider(1)

          val primaryKeyCol = userIdCol.getOrElse(idCol)

          def runCompareTest(ops: List[Op]) {
            using(SqlLoader(smartConn, rowPreparer(primaryKeyCol), false, dataSqlizer, NullLogger[TestColumnType, TestColumnValue], smartIds, smartVersions, executor, NoopReportWriter, NoopTimingReport)) { txn =>
              applyOps(txn, ops)
              txn.finish()
            }
            smartConn.commit()

            using(new StupidSqlLoader(stupidConn, rowPreparer(primaryKeyCol), false, stupidDataSqlizer, NullLogger[TestColumnType, TestColumnValue], stupidIds, stupidVersions, NoopReportWriter)) { txn =>
              applyOps(txn, ops)
              txn.finish()
            }
            stupidConn.commit()

            val smartData = for {
              smartStmt <- managed(smartConn.createStatement())
              smartRs <- managed(smartStmt.executeQuery(query(dataSqlizer.dataTableName)))
            } yield {
              result(smartRs)
            }

            val stupidData = for {
              stupidStmt <- managed(stupidConn.createStatement())
              stupidRs <- managed(stupidStmt.executeQuery(query(stupidDataSqlizer.dataTableName)))
            } yield {
              result(stupidRs)
            }

            smartData must equal (stupidData)
          }
          opss.foreach(runCompareTest)
        }
      }
    }
  }

  testOps("must contain the same data as a table manipulated by a StupidSqlLoader when user primary key exists", Tag("Slow")) { bySystemId =>
    val userIdCol = new ColumnId(3L)
    val userIdColName = "c_" + userIdCol.underlying

    val schema = ColumnIdMap(
      idCol -> new LongRep(idCol),
      versionCol -> new LongRep(versionCol),
      userIdCol -> new LongRep(userIdCol),
      num -> new LongRep(num),
      str -> new StringRep(str)
    )

    def genUpsert(genId: Gen[Long]) = for {
      id <- genId
      num <- Gen.frequency(2 -> Arbitrary.arbitrary[Long].map(Some(_)), 1 -> Gen.const(None))
      data <- Gen.frequency(2 -> Arbitrary.arbitrary[String].map(Some(_)), 1 -> Gen.const(None))
    } yield Upsert(Some(id), num, data.map(_.filterNot(_ == '\0')))

    def applyOps(txn: Loader[TestColumnValue], ops: List[Op]) {
      for((op, job) <- ops.zipWithIndex) op match {
        case Delete(id) => txn.delete(job, LongValue(id), None, bySystemIdForced = bySystemId)
        case Upsert(id, numV, data) => txn.upsert(job, Row(
          userIdCol -> LongValue(id.get), // always generated Some(id)
          num -> numV.map(LongValue).getOrElse(NullValue),
          str -> data.map(StringValue).getOrElse(NullValue)
        ), bySystemIdForced = bySystemId)
      }
    }

    def q(t: String) = "SELECT " + userIdColName + ", " + numName + ", " + strName + " FROM " + t + " ORDER BY " + userIdColName
    def result(rs: ResultSet): Vector[(Long, Long, Boolean, String)] = {
      val builder = new VectorBuilder[(Long, Long, Boolean, String)]
      while(rs.next()) {
        val id = rs.getLong(userIdColName)
        val num = rs.getLong(numName)
        val numWasNull = rs.wasNull()
        val str = rs.getString(strName)

        builder += ((id, num, numWasNull, str))
      }
      builder.result
    }

    testAgainstStupidSqlLoader[(Long, Long, Boolean, String)](schema, Some(userIdCol), genUpsert, applyOps, q, result)
  }


  testOps("must contain the same data as a table manipulated by a StupidSqlLoader when system id is the primary key", Tag("Slow")) { bySystemId =>
    def genUpsert(genId: Gen[Long]) = for {
      id <- Gen.frequency(1 -> genId.map(Some(_)), 2 -> Gen.const(None))
      num <- Gen.frequency(2 -> Arbitrary.arbitrary[Long].map(Some(_)), 1 -> Gen.const(None))
      data <- Gen.frequency(2 -> Arbitrary.arbitrary[String].map(Some(_)), 1 -> Gen.const(None))
    } yield Upsert(id, num, data.map(_.filterNot(_ == '\0')))

    def applyOps(txn: Loader[TestColumnValue], ops: List[Op]) {
      for ((op, job) <- ops.zipWithIndex) op match {
        case Delete(id) => txn.delete(job, LongValue(id), None, bySystemIdForced = bySystemId)
        case Upsert(id, numV, data) =>
          val baseRow = Row[TestColumnValue](
            num -> numV.map(LongValue).getOrElse(NullValue),
            str -> data.map(StringValue).getOrElse(NullValue)
          )
          val row = id match {
            case Some(sid) =>
              val newRow = new MutableColumnIdMap(baseRow)
              newRow(idCol) = LongValue(sid)
              newRow.freeze()
            case None => baseRow
          }
          txn.upsert(job, row, bySystemIdForced = bySystemId)
      }
    }

    def q(t: String) = "SELECT " + numName + ", " + strName + " FROM " + t + " ORDER BY " + numName + "," + strName
    def result(rs: ResultSet): Vector[(Long, Boolean, String)] = {
      val builder = new VectorBuilder[(Long, Boolean, String)]
      while (rs.next()) {
        val num = rs.getLong(numName)
        val numWasNull = rs.wasNull()
        val str = rs.getString(strName)

        builder += ((num, numWasNull, str))
      }
      builder.result
    }

    testAgainstStupidSqlLoader[(Long, Boolean, String)](standardSchema, None, genUpsert, applyOps, q, result)
  }
}
