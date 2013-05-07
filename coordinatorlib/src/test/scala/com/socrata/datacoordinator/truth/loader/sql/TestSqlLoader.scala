package com.socrata.datacoordinator
package truth.loader
package sql

import org.scalatest.{Tag, FunSuite, BeforeAndAfterAll}

import scala.collection.immutable.VectorBuilder

import java.sql.{Connection, DriverManager}

import org.scalatest.matchers.MustMatchers
import org.scalatest.prop.PropertyChecks
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.util.{RowVersionProvider, RowIdProvider, RowDataProvider, NoopTimingReport}
import com.socrata.datacoordinator.id.{RowVersion, RowId, ColumnId}
import com.socrata.datacoordinator.util.collection.{MutableColumnIdMap, ColumnIdMap}
import com.rojoma.json.ast.{JString, JNumber, JValue}

class TestSqlLoader extends FunSuite with MustMatchers with PropertyChecks with BeforeAndAfterAll {
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

  test("adding a new row with system PK succeeds") {
    val dsContext = new TestDatasetContext(standardSchema, idCol, None, versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), dataSqlizer, dataLogger, idProvider(15), versionProvider(53), executor, NoopTimingReport))
      } {
        txn.upsert(0, Row[TestColumnValue](num -> LongValue(1), str -> StringValue("a")))
        val report = txn.report
        dataLogger.finish()

        report.inserted must equal (Map(0 -> IdAndVersion(LongValue(15), new RowVersion(53))))
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

  test("inserting and then updating a new row with system PK succeeds") {
    val ids = idProvider(15)
    val vers = versionProvider(431)
    val dsContext = new TestDatasetContext(standardSchema, idCol, None, versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), dataSqlizer, dataLogger, ids, vers, executor, NoopTimingReport))
      } {
        txn.upsert(0, Row[TestColumnValue](num -> LongValue(1), str -> StringValue("a")))
        txn.upsert(1, Row[TestColumnValue](idCol -> LongValue(15), num -> LongValue(2), str -> StringValue("b")))
        val report = txn.report
        dataLogger.finish()

        report.inserted must equal (Map(0 -> IdAndVersion(LongValue(15), new RowVersion(431))))
        report.updated must be (Map(1 -> IdAndVersion(LongValue(15), new RowVersion(432))))
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
          val op2 = logMap(idCol -> JNumber(15), versionCol -> JNumber(432), num -> JNumber(2), str -> JString("b"))
          Map("version" -> 1L, "subversion" -> 1L, "rows" -> ("""[{"i":""" + op1 + """},{"u":""" + op2 + """}]"""), "who" -> "hello")
        }
      ))
    }
  }

  test("trying to add a new row with a NULL system PK does not fail (it adds a row with a generated ID)") {
    val dsContext = new TestDatasetContext(standardSchema, idCol, None, versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      val ids = idProvider(22)
      val vers = versionProvider(111)

      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), dataSqlizer, dataLogger, ids, vers, executor, NoopTimingReport))
      } {
        txn.upsert(0, Row(idCol -> NullValue, num -> LongValue(1), str -> StringValue("a")))
        val report = txn.report
        dataLogger.finish()

        report.inserted must be (Map(0 -> IdAndVersion(LongValue(22), new RowVersion(111))))
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

  test("trying to add a new row with a nonexistant system PK fails") {
    val dsContext = new TestDatasetContext(standardSchema, idCol, None, versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      val ids = idProvider(6)
      val vers = versionProvider(99)

      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), dataSqlizer, dataLogger, ids, vers, executor, NoopTimingReport))
      } {
        txn.upsert(0, Row(idCol -> LongValue(77), num -> LongValue(1), str -> StringValue("a")))
        val report = txn.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must equal (Map(0 -> NoSuchRowToUpdate(LongValue(77))))
      }
      conn.commit()

      ids.underlying.finish() must be (6L)
      vers.underlying.finish() must be (99L)

      query(conn, rawSelect) must equal (Seq.empty)
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq.empty)
    }
  }

  test("updating an existing row by system id succeeds") {
    val ids = idProvider(13)
    val vers = versionProvider(5)
    val dsContext = new TestDatasetContext(standardSchema, idCol, None, versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      preload(conn, dataSqlizer, standardLogTableName)(
        Row(idCol -> LongValue(7), versionCol -> LongValue(99), str -> StringValue("q"), num -> LongValue(2))
      )
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), dataSqlizer, dataLogger, ids, vers, executor, NoopTimingReport))
      } {
        txn.upsert(0, Row(idCol -> LongValue(7), num -> LongValue(44)))
        val report = txn.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must equal (Map(0 -> IdAndVersion(LongValue(7), new RowVersion(5))))
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
          val op = logMap(idCol -> JNumber(7), versionCol -> JNumber(5), num -> JNumber(44), str -> JString("q"))
          Map("version" -> 1L, "subversion" -> 1L, "rows" -> ("""[{"u":""" + op + """}]"""), "who" -> "hello")
        }
      ))
    }
  }

  test("adding a new row with user PK succeeds") {
    val ids = idProvider(15)
    val vers = versionProvider(999999)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName))
        txn <- managed(SqlLoader(conn, rowPreparer(str), dataSqlizer, dataLogger, ids, vers, executor, NoopTimingReport))
      } {
        txn.upsert(0, Row(num -> LongValue(1), str -> StringValue("a")))
        val report = txn.report
        dataLogger.finish()

        report.inserted must equal (Map(0 -> IdAndVersion(StringValue("a"), new RowVersion(999999))))
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

  test("trying to add a new row with a NULL user PK fails") {
    val ids = idProvider(22)
    val vers = versionProvider(8)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName))
        txn <- managed(SqlLoader(conn, rowPreparer(str), dataSqlizer, dataLogger, ids, vers, executor, NoopTimingReport))
      } {
        txn.upsert(0, Row(num -> LongValue(1), str -> NullValue))
        val report = txn.report
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

  test("trying to add a new row without user PK fails") {
    val ids = idProvider(22)
    val vers = versionProvider(16)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName))
        txn <- managed(SqlLoader(conn, rowPreparer(str), dataSqlizer, dataLogger, ids, vers, executor, NoopTimingReport))
      } {
        txn.upsert(0, Row(num -> LongValue(1)))
        val report = txn.report
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

  test("updating an existing row by user pk succeeds") {
    val ids = idProvider(13)
    val vers = versionProvider(24)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      preload(conn, dataSqlizer, standardLogTableName)(
        Row(idCol -> LongValue(7), versionCol -> LongValue(11001001), str -> StringValue("q"), num -> LongValue(2))
      )
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName))
        txn <- managed(SqlLoader(conn, rowPreparer(str), dataSqlizer, dataLogger, ids, vers, executor, NoopTimingReport))
      } {
        txn.upsert(0, Row(str -> StringValue("q"), num -> LongValue(44)))
        val report = txn.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must equal (Map(0 -> IdAndVersion(StringValue("q"), new RowVersion(24))))
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
          val op = logMap(idCol -> JNumber(7), versionCol -> JNumber(24), num -> JNumber(44), str -> JString("q"))
          Map("version" -> 1L, "subversion" -> 1L, "rows" -> ("""[{"u":""" + op + """}]"""), "who" -> "hello")
        }
      ))
    }
  }

  test("inserting and then updating a new row with user PK succeeds") {
    val ids = idProvider(15)
    val vers = versionProvider(32)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName))
        txn <- managed(SqlLoader(conn, rowPreparer(str), dataSqlizer, dataLogger, ids, vers, executor, NoopTimingReport))
      } {
        txn.upsert(0, Row(num -> LongValue(1), str -> StringValue("q")))
        txn.upsert(1, Row(num -> LongValue(2), str -> StringValue("q")))
        val report = txn.report
        dataLogger.finish()

        report.inserted must equal (Map(0 -> IdAndVersion(StringValue("q"), new RowVersion(32))))
        report.updated must be (Map(1 -> IdAndVersion(StringValue("q"), new RowVersion(33))))
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
          val op2 = logMap(idCol -> JNumber(15), versionCol -> JNumber(33), num -> JNumber(2), str -> JString("q"))
          Map("version" -> 1L, "subversion" -> 1L, "rows" -> ("""[{"i":""" + op1 + """},{"u":""" + op2 + """}]"""), "who" -> "hello")
        }
      ))
    }
  }

  test("specifying :id when there's a user PK succeeds (and ignores it)") {
    val ids = idProvider(15)
    val vers = versionProvider(40)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName))
        txn <- managed(SqlLoader(conn, rowPreparer(str), dataSqlizer, dataLogger, ids, vers, executor, NoopTimingReport))
      } {
        txn.upsert(0, Row(idCol -> LongValue(15), num -> LongValue(1), str -> StringValue("q")))
        val report = txn.report
        dataLogger.finish()

        report.inserted must be (Map(0 -> IdAndVersion(StringValue("q"), new RowVersion(40))))
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

  test("deleting a row just inserted with a user PK succeeds") {
    val ids = idProvider(15)
    val vers = versionProvider(48)
    val dsContext = new TestDatasetContext(standardSchema, idCol, Some(str), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName))
        txn <- managed(SqlLoader(conn, rowPreparer(str), dataSqlizer, dataLogger, ids, vers, executor, NoopTimingReport))
      } {
        txn.upsert(0, Row(num -> LongValue(1), str -> StringValue("q")))
        txn.delete(1, StringValue("q"), None)
        val report = txn.report
        dataLogger.finish()

        report.inserted must be (Map(0 -> IdAndVersion(StringValue("q"), new RowVersion(48))))
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
          Map("version" -> 1L, "subversion" -> 1L, "rows" -> ("""[{"i":""" + op + """},{"d":15}]"""), "who" -> "hello")
        }
      ))
    }
  }

  test("deleting a row just inserted with a system PK succeeds") {
    // This isn't a useful thing to be able to do, since in the real system
    // IDs won't be user-predictable, but it's a valuable sanity check anyway.
    val ids = idProvider(15)
    val vers = versionProvider(56)
    val dsContext = new TestDatasetContext(standardSchema, idCol, None, versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer, standardLogTableName)
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, standardLogTableName))
        txn <- managed(SqlLoader(conn, rowPreparer(idCol), dataSqlizer, dataLogger, ids, vers, executor, NoopTimingReport))
      } {
        txn.upsert(0, Row(num -> LongValue(1), str -> StringValue("q")))
        txn.delete(1, LongValue(15), None)
        val report = txn.report
        dataLogger.finish()

        report.inserted must be (Map(0 -> IdAndVersion(LongValue(15), new RowVersion(56))))
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
          Map("version" -> 1L, "subversion" -> 1L, "rows" -> ("""[{"i":""" + op + """},{"d":15}]"""), "who" -> "hello")
        }
      ))
    }
  }

  test("inserting a row with an explicitly null version succeeds (system PK)")(pending)
  test("inserting a row with an explicitly null version succeeds (user PK)")(pending)
  test("inserting a row with an not-null version fails (system PK)")(pending)
  test("inserting a row with an not-null version fails (user PK)")(pending)
  test("updating a row with an explicitly null version fails (system PK)")(pending)
  test("updating a row with an explicitly null version fails (user PK)")(pending)
  test("updating a row with an incorrect not-null version fails (system PK)")(pending)
  test("updating a row with an incorrect not-null version fails (user PK)")(pending)
  test("updating a row with a correct not-null version succeeds (system PK)")(pending)
  test("updating a row with a correct not-null version succeeds (user PK)")(pending)

  test("must contain the same data as a table manipulated by a StupidPostgresTransaction when using user IDs", Tag("Slow")) {
    import org.scalacheck.{Gen, Arbitrary}

    sealed abstract class Op
    case class Upsert(id: Long, num: Option[Long], data: Option[String]) extends Op
    case class Delete(id: Long) extends Op

    val genId = Gen.choose(0L, 100L)

    val genUpsert = for {
      id <- genId
      num <- Gen.frequency(2 -> Arbitrary.arbitrary[Long].map(Some(_)), 1 -> Gen.value(None))
      data <- Gen.frequency(2 -> Arbitrary.arbitrary[String].map(Some(_)), 1 -> Gen.value(None))
    } yield Upsert(id, num, data.map(_.filterNot(_ == '\0')))

    val genDelete = genId.map(Delete(_))

    val genOp = Gen.frequency(2 -> genUpsert, 1 -> genDelete)

    implicit val arbOp = Arbitrary[Op](genOp)

    val ids = idProvider(1)
    val vers = versionProvider(1)

    val userIdCol = new ColumnId(3L)
    val userIdColName = "c_" + userIdCol.underlying

    val schema = ColumnIdMap(
      idCol -> new LongRep(idCol),
      versionCol -> new LongRep(versionCol),
      userIdCol -> new LongRep(userIdCol),
      num -> new LongRep(num),
      str -> new StringRep(str)
    )

    val dsContext = new TestDatasetContext(schema, idCol, Some(userIdCol), versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    val stupidDsContext = new TestDatasetContext(schema, idCol, Some(userIdCol), versionCol)
    val stupidDataSqlizer = new TestDataSqlizer("stupid_data", stupidDsContext)

    def applyOps(txn: Loader[TestColumnValue], ops: List[Op]) {
      for((op, job) <- ops.zipWithIndex) op match {
        case Delete(id) => txn.delete(job, LongValue(id), None)
        case Upsert(id, numV, data) => txn.upsert(job, Row(
          userIdCol -> LongValue(id),
          num -> numV.map(LongValue(_)).getOrElse(NullValue),
          str -> data.map(StringValue(_)).getOrElse(NullValue)
        ))
      }
    }

    forAll { (opss: List[List[Op]]) =>
      withDB() { stupidConn =>
        withDB() { smartConn =>
          makeTables(smartConn, dataSqlizer, standardLogTableName)
          makeTables(stupidConn, stupidDataSqlizer, "stupid_log")

          def runCompareTest(ops: List[Op]) {
            val smartReport = using(SqlLoader(smartConn, rowPreparer(userIdCol), dataSqlizer, NullLogger[TestColumnType, TestColumnValue], ids, vers, executor, NoopTimingReport)) { txn =>
              applyOps(txn, ops)
              txn.report
            }
            smartConn.commit()

            val stupidReport = using(new StupidSqlLoader(stupidConn, rowPreparer(userIdCol), stupidDataSqlizer, NullLogger[TestColumnType, TestColumnValue], ids, vers)) { txn =>
              applyOps(txn, ops)
              txn.report
            }
            stupidConn.commit()

            def q(t: String) = "SELECT " + userIdColName + ", " + numName + ", " + strName + " FROM " + t + " ORDER BY " + userIdColName
            /* -- "must" is too expensive to call in an inner loop
              for {
                smartStmt <- managed(smartConn.createStatement())
                smartRs <- managed(smartStmt.executeQuery(q))
                stupidStmt <- managed(stupidConn.createStatement())
                stupidRs <- managed(stupidStmt.executeQuery(q))
              } {
                while(smartRs.next()) {
                  stupidRs.next() must be (true)
                  smartRs.getLong("ID") must equal (stupidRs.getLong("ID"))
                  smartRs.getLong("NUM") must equal (stupidRs.getLong("NUM"))
                  smartRs.wasNull() must equal (stupidRs.wasNull())
                  smartRs.getString("STR") must equal (stupidRs.getString("STR"))
                }
                stupidRs.next() must be (false)
              }
 */
            val smartData = for {
              smartStmt <- managed(smartConn.createStatement())
              smartRs <- managed(smartStmt.executeQuery(q(dataSqlizer.dataTableName)))
            } yield {
              val fromSmart = new VectorBuilder[(Long, Long, Boolean, String)]
              while(smartRs.next()) {
                val id = smartRs.getLong(userIdColName)
                val num = smartRs.getLong(numName)
                val numWasNull = smartRs.wasNull()
                val str = smartRs.getString(strName)

                fromSmart += ((id, num, numWasNull, str))
              }
              fromSmart.result
            }

            val stupidData = for {
              stupidStmt <- managed(stupidConn.createStatement())
              stupidRs <- managed(stupidStmt.executeQuery(q(stupidDataSqlizer.dataTableName)))
            } yield {
              val fromStupid = new VectorBuilder[(Long, Long, Boolean, String)]
              while(stupidRs.next()) {
                val id = stupidRs.getLong(userIdColName)
                val num = stupidRs.getLong(numName)
                val numWasNull = stupidRs.wasNull()
                val str = stupidRs.getString(strName)

                fromStupid += ((id, num, numWasNull, str))
              }
              fromStupid.result
            }

            smartData must equal (stupidData)
          }

          opss.foreach(runCompareTest)
        }
      }
    }
  }

  test("must contain the same data as a table manipulated by a StupidPostgresTransaction when using system IDs", Tag("Slow")) {
    import org.scalacheck.{Gen, Arbitrary}

    sealed abstract class Op
    case class Upsert(id: Option[Long], num: Option[Long], data: Option[String]) extends Op
    case class Delete(id: Long) extends Op

    val genId = Gen.choose(0L, 100L)

    val genUpsert = for {
      id <- Gen.frequency(1 -> genId.map(Some(_)), 2 -> Gen.value(None))
      num <- Gen.frequency(2 -> Arbitrary.arbitrary[Long].map(Some(_)), 1 -> Gen.value(None))
      data <- Gen.frequency(2 -> Arbitrary.arbitrary[String].map(Some(_)), 1 -> Gen.value(None))
    } yield Upsert(id, num, data.map(_.filterNot(_ == '\0')))

    val genDelete = genId.map(Delete(_))

    val genOp = Gen.frequency(2 -> genUpsert, 1 -> genDelete)

    implicit val arbOp = Arbitrary[Op](genOp)

    val dsContext = new TestDatasetContext(standardSchema, idCol, None, versionCol)
    val dataSqlizer = new TestDataSqlizer(standardTableName, dsContext)

    val stupidDsContext = new TestDatasetContext(standardSchema, idCol, None, versionCol)
    val stupidDataSqlizer = new TestDataSqlizer("stupid_data", stupidDsContext)

    def applyOps(txn: Loader[TestColumnValue], ops: List[Op]) {
      for((op, job) <- ops.zipWithIndex) op match {
        case Delete(id) => txn.delete(job, LongValue(id), None)
        case Upsert(id, numV, data) =>
          val baseRow = Row[TestColumnValue](
            num -> numV.map(LongValue(_)).getOrElse(NullValue),
            str -> data.map(StringValue(_)).getOrElse(NullValue)
          )
          val row = id match {
            case Some(sid) =>
              val newRow = new MutableColumnIdMap(baseRow)
              newRow(idCol) = LongValue(sid)
              newRow.freeze()
            case None => baseRow
          }
          txn.upsert(job, row)
      }
    }

    forAll { (opss: List[List[Op]]) =>
      withDB() { stupidConn =>
        withDB() { smartConn =>
          makeTables(smartConn, dataSqlizer, standardLogTableName)
          makeTables(stupidConn, stupidDataSqlizer, "stupid_log")

          val smartIds = idProvider(1)
          val stupidIds = idProvider(1)

          val smartVersions = versionProvider(1)
          val stupidVersions = versionProvider(1)

          def runCompareTest(ops: List[Op]) {
            val smartReport =
              using(SqlLoader(smartConn, rowPreparer(idCol), dataSqlizer, NullLogger[TestColumnType, TestColumnValue], smartIds, smartVersions, executor, NoopTimingReport)) { txn =>
                applyOps(txn, ops)
                txn.report
              }
            smartConn.commit()

            val stupidReport =
              using(new StupidSqlLoader(stupidConn, rowPreparer(idCol), stupidDataSqlizer, NullLogger[TestColumnType, TestColumnValue], stupidIds, stupidVersions)) { txn =>
                applyOps(txn, ops)
                txn.report
              }
            stupidConn.commit()

            def q(t: String) = "SELECT " + numName + ", " + strName + " FROM " + t + " ORDER BY " + numName + "," + strName
/* -- "must" is too expensive to call in an inner loop
            val q = "SELECT u_num AS NUM, u_str AS STR FROM test_data ORDER BY u_num, u_str"
            for {
              smartStmt <- managed(smartConn.createStatement())
              smartRs <- managed(smartStmt.executeQuery(q))
              stupidStmt <- managed(stupidConn.createStatement())
              stupidRs <- managed(stupidStmt.executeQuery(q))
            } {
              while(smartRs.next()) {
                stupidRs.next() must be (true)
                smartRs.getLong("NUM") must equal (stupidRs.getLong("NUM"))
                smartRs.wasNull() must equal (stupidRs.wasNull())
                smartRs.getString("STR") must equal (stupidRs.getString("STR"))
              }
              stupidRs.next() must be (false)
            }
*/
            val smartData = for {
              smartStmt <- managed(smartConn.createStatement())
              smartRs <- managed(smartStmt.executeQuery(q(dataSqlizer.dataTableName)))
            } yield {
              val fromSmart = new VectorBuilder[(Long, Boolean, String)]
              while(smartRs.next()) {
                val num = smartRs.getLong(numName)
                val numWasNull = smartRs.wasNull()
                val str = smartRs.getString(strName)

                fromSmart += ((num, numWasNull, str))
              }
              fromSmart.result
            }

            val stupidData = for {
              stupidStmt <- managed(stupidConn.createStatement())
              stupidRs <- managed(stupidStmt.executeQuery(q(stupidDataSqlizer.dataTableName)))
            } yield {
              val fromStupid = new VectorBuilder[(Long, Boolean, String)]
              while(stupidRs.next()) {
                val num = stupidRs.getLong(numName)
                val numWasNull = stupidRs.wasNull()
                val str = stupidRs.getString(strName)

                fromStupid += ((num, numWasNull, str))
              }
              fromStupid.result
            }

            smartData must equal (stupidData)
          }
          opss.foreach(runCompareTest)
        }
      }
    }
  }
}
