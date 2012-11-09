package com.socrata.datacoordinator.loader

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import com.rojoma.simplearm.util._

import com.socrata.id.numeric.{PushbackIdProvider, FixedSizeIdProvider, InMemoryBlockIdProvider}
import java.sql.{Connection, DriverManager}
import collection.immutable.VectorBuilder

class TestPostgresTransaction extends FunSuite with MustMatchers {
  def idProvider(initial: Int) = new PushbackIdProvider(new FixedSizeIdProvider(new InMemoryBlockIdProvider(releasable = false) { override def start = initial }, 1024))

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
          row + (rs.getMetaData.getColumnLabel(col) -> rs.getObject(col))
        }
      }
      result.result()
    }

  def makeTables(conn: Connection, ctx: DatasetContext[TestColumnType, TestColumnValue]) {
    execute(conn, "CREATE TABLE " + ctx.baseName + "_data (id bigint not null primary key," + ctx.userSchema.map { case (c,t) =>
      val sqltype = t match {
        case LongColumn => "BIGINT"
        case StringColumn => "VARCHAR(100)"
      }
      "u_" + c + " " + sqltype + (if(ctx.userPrimaryKeyColumn == Some(c)) " NOT NULL" else " NULL")
    }.mkString(",") + ")")
    ctx.userPrimaryKeyColumn.foreach { pkCol =>
      execute(conn, "CREATE INDEX " + ctx.baseName + "_data_userid ON " + ctx.baseName + "_data(u_" + pkCol + ")")
    }
    execute(conn, "CREATE TABLE " + ctx.baseName + "_log (id serial not null primary key, row bigint not null, who varchar(100) null)")
  }

  def preload(conn: Connection, ctx: DatasetContext[TestColumnType, TestColumnValue])(rows: Map[String, TestColumnValue]*) {
    makeTables(conn, ctx)
    for(row <- rows) {
      val LongValue(id) = row.getOrElse(ctx.systemIdColumnName, sys.error("No :id"))
      val remainingColumns = row - ctx.systemIdColumnName
      assert(remainingColumns.keySet.subsetOf(ctx.userSchema.keySet), "row contains extraneous keys")
      val sql = "insert into " + ctx.baseName + "_data (id," + remainingColumns.keys.map("u_" + _).mkString(",") + ") values (" + id + "," + remainingColumns.values.map(_.sqlize).mkString(",") + ")"
      execute(conn, sql)
    }
  }

  val standardTableName = "test"
  val standardSchema = Map("num" -> LongColumn, "str" -> StringColumn)

  test("adding a new row with system PK succeeds") {
    val dsContext = new TestDatasetContext(standardTableName, standardSchema, None)
    val dataSqlizer = new TestDataSqlizer("hello", dsContext)

    withDB() { conn =>
      makeTables(conn, dsContext)
      conn.commit()

      val txn = PostgresTransaction(conn, TestTypeContext, dataSqlizer, idProvider(15))
      txn.upsert(Map("num" -> LongValue(1), "str" -> StringValue("a")))
      val report = txn.report
      report.inserted must equal (Map(0 -> LongValue(15)))
      report.updated must be ('empty)
      report.deleted must be ('empty)
      report.errors must be ('empty)
      report.elided must be ('empty)
      txn.commit()

      query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq(Map("ID" -> 15L, "NUM" -> 1L, "STR" -> "a")))
      query(conn, "SELECT id as ID, row as ROW, who as WHO from test_log") must equal (Seq(Map("ID" -> 1, "ROW" -> 15L, "WHO" -> "hello")))
    }
  }

  test("inserting and then updating a new row with system PK succeeds") {
    val ids = idProvider(15)
    val dsContext = new TestDatasetContext(standardTableName, standardSchema, None)
    val dataSqlizer = new TestDataSqlizer("hello", dsContext)

    withDB() { conn =>
      makeTables(conn, dsContext)
      conn.commit()

      val txn = PostgresTransaction(conn, TestTypeContext, dataSqlizer, ids)
      txn.upsert(Map("num" -> LongValue(1), "str" -> StringValue("a")))
      txn.upsert(Map(":id" -> LongValue(15), "num" -> LongValue(2), "str" -> StringValue("b")))
      val report = txn.report
      report.inserted must equal (Map(0 -> LongValue(15)))
      report.updated must be ('empty)
      report.deleted must be ('empty)
      report.errors must be ('empty)
      report.elided must equal (Map(1 -> (LongValue(15), 0)))
      txn.commit()

      ids.allocate() must be (16)

      query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq(Map("ID" -> 15L, "NUM" -> 2L, "STR" -> "b")))
      query(conn, "SELECT id as ID, row as ROW, who as WHO from test_log") must equal (Seq(
        Map("ID" -> 1, "ROW" -> 15L, "WHO" -> "hello")
      ))
    }
  }

  test("trying to add a new row with a NULL system PK fails") {
    val dsContext = new TestDatasetContext(standardTableName, standardSchema, None)
    val dataSqlizer = new TestDataSqlizer("hello", dsContext)

    withDB() { conn =>
      makeTables(conn, dsContext)
      conn.commit()

      val txn = PostgresTransaction(conn, TestTypeContext, dataSqlizer, idProvider(22))
      txn.upsert(Map(":id" -> NullValue, "num" -> LongValue(1), "str" -> StringValue("a")))
      val report = txn.report
      report.inserted must be ('empty)
      report.updated must be ('empty)
      report.deleted must be ('empty)
      report.errors must equal (Map(0 -> NullPrimaryKey))
      report.elided must be ('empty)
      txn.commit()

      query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq.empty)
      query(conn, "SELECT id as ID, row as ROW, who as WHO from test_log") must equal (Seq.empty)
    }
  }

  test("trying to add a new row with a nonexistant system PK fails") {
    val dsContext = new TestDatasetContext(standardTableName, standardSchema, None)
    val dataSqlizer = new TestDataSqlizer("hello", dsContext)

    withDB() { conn =>
      makeTables(conn, dsContext)
      conn.commit()

      val txn = PostgresTransaction(conn, TestTypeContext, dataSqlizer, idProvider(6))
      txn.upsert(Map(":id" -> LongValue(77), "num" -> LongValue(1), "str" -> StringValue("a")))
      val report = txn.report
      report.inserted must be ('empty)
      report.updated must be ('empty)
      report.deleted must be ('empty)
      report.errors must equal (Map(0 -> NoSuchRowToUpdate(LongValue(77))))
      report.elided must be ('empty)
      txn.commit()

      query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq.empty)
      query(conn, "SELECT id as ID, row as ROW, who as WHO from test_log") must equal (Seq.empty)
    }
  }

  test("updating an existing row by system id succeeds") {
    val ids = idProvider(13)
    val dsContext = new TestDatasetContext(standardTableName, standardSchema, None)
    val dataSqlizer = new TestDataSqlizer("hello", dsContext)

    withDB() { conn =>
      preload(conn, dsContext)(
        Map(":id" -> LongValue(7), "str" -> StringValue("q"), "num" -> LongValue(2))
      )
      conn.commit()

      val txn = PostgresTransaction(conn, TestTypeContext, dataSqlizer, ids)
      txn.upsert(Map(":id" -> LongValue(7), "num" -> LongValue(44)))
      val report = txn.report
      report.inserted must be ('empty)
      report.updated must equal (Map(0 -> LongValue(7)))
      report.deleted must be ('empty)
      report.errors must be ('empty)
      report.elided must be ('empty)
      txn.commit()

      ids.allocate() must be (13)

      query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq(Map("ID" -> 7L, "NUM" -> 44L, "STR" -> "q")))
      query(conn, "SELECT id as ID, row as ROW, who as WHO from test_log") must equal (Seq(Map("ID" -> 1, "ROW" -> 7L, "WHO" -> "hello")))
    }
  }

  def userCast[A,B](t: PostgresTransaction[A,B]) = t.asInstanceOf[UserPKPostgresTransaction[A,B]]

  test("adding a new row with user PK succeeds") {
    for(insertFirst <- List(true, false)) {
      val ids = idProvider(15)
      val dsContext = new TestDatasetContext(standardTableName, standardSchema, Some("str"))
      val dataSqlizer = new TestDataSqlizer("hello", dsContext)

      withDB() { conn =>
        makeTables(conn, dsContext)
        conn.commit()

        val txn = userCast(PostgresTransaction(conn, TestTypeContext, dataSqlizer, ids))
        txn.tryInsertFirst = insertFirst
        txn.upsert(Map("num" -> LongValue(1), "str" -> StringValue("a")))
        val report = txn.report
        report.inserted must equal (Map(0 -> StringValue("a")))
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be ('empty)
        report.elided must be ('empty)
        txn.commit()

        ids.allocate() must equal (16)

        query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq(Map("ID" -> 15L, "NUM" -> 1L, "STR" -> "a")))
        query(conn, "SELECT id as ID, row as ROW, who as WHO from test_log") must equal (Seq(Map("ID" -> 1, "ROW" -> 15L, "WHO" -> "hello")))
      }
    }
  }

  test("trying to add a new row with a NULL user PK fails") {
    for(insertFirst <- List(true, false)) {
      val ids = idProvider(22)
      val dsContext = new TestDatasetContext(standardTableName, standardSchema, Some("str"))
      val dataSqlizer = new TestDataSqlizer("hello", dsContext)

      withDB() { conn =>
        makeTables(conn, dsContext)
        conn.commit()

        val txn = userCast(PostgresTransaction(conn, TestTypeContext, dataSqlizer, ids))
        txn.tryInsertFirst = insertFirst
        txn.upsert(Map("num" -> LongValue(1), "str" -> NullValue))
        val report = txn.report
        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must equal (Map(0 -> NullPrimaryKey))
        report.elided must be ('empty)
        txn.commit()

        ids.allocate() must equal (22)

        query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq.empty)
        query(conn, "SELECT id as ID, row as ROW, who as WHO from test_log") must equal (Seq.empty)
      }
    }
  }

  test("trying to add a new row without user PK fails") {
    for(insertFirst <- List(true, false)) {
      val ids = idProvider(22)
      val dsContext = new TestDatasetContext(standardTableName, standardSchema, Some("str"))
      val dataSqlizer = new TestDataSqlizer("hello", dsContext)

      withDB() { conn =>
        makeTables(conn, dsContext)
        conn.commit()

        val txn = userCast(PostgresTransaction(conn, TestTypeContext, dataSqlizer, ids))
        txn.tryInsertFirst = insertFirst
        txn.upsert(Map("num" -> LongValue(1)))
        val report = txn.report
        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be (Map(0 -> NoPrimaryKey))
        report.elided must be ('empty)
        txn.commit()

        ids.allocate() must equal (22)

        query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq.empty)
        query(conn, "SELECT id as ID, row as ROW, who as WHO from test_log") must equal (Seq.empty)
      }
    }
  }

  test("updating an existing row by user pk succeeds") {
    for(insertFirst <- List(true, false)) {
      val ids = idProvider(13)
      val dsContext = new TestDatasetContext(standardTableName, standardSchema, Some("str"))
      val dataSqlizer = new TestDataSqlizer("hello", dsContext)

      withDB() { conn =>
        preload(conn, dsContext)(
          Map(":id" -> LongValue(7), "str" -> StringValue("q"), "num" -> LongValue(2))
        )
        conn.commit()

        val txn = userCast(PostgresTransaction(conn, TestTypeContext, dataSqlizer, ids))
        txn.tryInsertFirst = insertFirst
        txn.upsert(Map("str" -> StringValue("q"), "num" -> LongValue(44)))
        val report = txn.report
        report.inserted must be ('empty)
        report.updated must equal (Map(0 -> StringValue("q")))
        report.deleted must be ('empty)
        report.errors must be ('empty)
        report.elided must be ('empty)
        txn.commit()

        ids.allocate() must be (13)

        query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq(Map("ID" -> 7L, "NUM" -> 44L, "STR" -> "q")))
        query(conn, "SELECT id as ID, row as ROW, who as WHO from test_log") must equal (Seq(
          Map("ID" -> 1, "ROW" -> 7L, "WHO" -> "hello")
        ))
      }
    }
  }

  test("inserting and then updating a new row with user PK succeeds") {
    for(insertFirst <- List(true, false)) {
      val ids = idProvider(15)
      val dsContext = new TestDatasetContext(standardTableName, standardSchema, Some("str"))
      val dataSqlizer = new TestDataSqlizer("hello", dsContext)

      withDB() { conn =>
        makeTables(conn, dsContext)
        conn.commit()

        val txn = userCast(PostgresTransaction(conn, TestTypeContext, dataSqlizer, ids))
        txn.tryInsertFirst = insertFirst
        txn.upsert(Map("num" -> LongValue(1), "str" -> StringValue("q")))
        txn.upsert(Map("num" -> LongValue(2), "str" -> StringValue("q")))
        val report = txn.report
        report.inserted must equal (Map(0 -> StringValue("q")))
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be ('empty)
        report.elided must equal (Map(1 -> (StringValue("q"), 0)))
        txn.commit()

        ids.allocate() must be (16)

        query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq(Map("ID" -> 15L, "NUM" -> 2L, "STR" -> "q")))
        query(conn, "SELECT id as ID, row as ROW, who as WHO from test_log") must equal (Seq(
          Map("ID" -> 1, "ROW" -> 15L, "WHO" -> "hello")
        ))
      }
    }
  }

  test("specifying :id when there's a user PK fails") {
    for(insertFirst <- List(true, false)) {
      val ids = idProvider(15)
      val dsContext = new TestDatasetContext(standardTableName, standardSchema, Some("str"))
      val dataSqlizer = new TestDataSqlizer("hello", dsContext)

      withDB() { conn =>
        makeTables(conn, dsContext)
        conn.commit()

        val txn = userCast(PostgresTransaction(conn, TestTypeContext, dataSqlizer, ids))
        txn.tryInsertFirst = insertFirst
        txn.upsert(Map(":id" -> LongValue(15), "num" -> LongValue(1), "str" -> StringValue("q")))
        val report = txn.report
        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must equal (Map(0 -> SystemColumnsSet(Set(":id"))))
        report.elided must be ('empty)
        txn.commit()

        ids.allocate() must be (15)

        query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq.empty)
        query(conn, "SELECT id as ID, row as ROW, who as WHO from test_log") must equal (Seq.empty)
      }
    }
  }

  test("deleting a row just inserted with a user PK succeeds") {
    for(insertFirst <- List(true, false)) {
      val ids = idProvider(15)
      val dsContext = new TestDatasetContext(standardTableName, standardSchema, Some("str"))
      val dataSqlizer = new TestDataSqlizer("hello", dsContext)

      withDB() { conn =>
        makeTables(conn, dsContext)
        conn.commit()

        val txn = userCast(PostgresTransaction(conn, TestTypeContext, dataSqlizer, ids))
        txn.tryInsertFirst = insertFirst
        txn.upsert(Map("num" -> LongValue(1), "str" -> StringValue("q")))
        txn.delete(StringValue("q"))
        val report = txn.report
        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must equal (Map(1 -> StringValue("q")))
        report.elided must equal (Map(0 -> (StringValue("q"), 1)))
        report.errors must be ('empty)
        txn.commit()

        ids.allocate() must be (15) // and it never even allocated a sid for it

        query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq.empty)
        query(conn, "SELECT id as ID, row as ROW, who as WHO from test_log") must equal (Seq.empty)
      }
    }
  }

  test("deleting a row just inserted with a system PK succeeds") {
    // This isn't a useful thing to be able to do, since in the real system
    // IDs won't be user-predictable, but it's a valuable sanity check anyway.
    val ids = idProvider(15)
    val dsContext = new TestDatasetContext(standardTableName, standardSchema, None)
    val dataSqlizer = new TestDataSqlizer("hello", dsContext)

    withDB() { conn =>
      makeTables(conn, dsContext)
      conn.commit()

      val txn = PostgresTransaction(conn, TestTypeContext, dataSqlizer, ids)
      txn.upsert(Map("num" -> LongValue(1), "str" -> StringValue("q")))
      txn.delete(LongValue(15))
      val report = txn.report
      report.inserted must be ('empty)
      report.updated must be ('empty)
      report.deleted must equal (Map(1 -> LongValue(15)))
      report.errors must be ('empty)
      report.elided must equal (Map(0 -> (LongValue(15), 1)))
      txn.commit()

      ids.allocate() must be (15)

      query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq.empty)
      query(conn, "SELECT id as ID, row as ROW, who as WHO from test_log") must equal (Seq.empty)
    }
  }
}
