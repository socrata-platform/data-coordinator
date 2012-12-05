package com.socrata.datacoordinator
package truth.loader
package sql

import scala.collection.immutable.VectorBuilder

import java.sql.{Connection, DriverManager}

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.scalatest.matchers.MustMatchers
import org.scalatest.prop.PropertyChecks
import com.rojoma.simplearm.util._

import com.socrata.id.numeric.{PushbackIdProvider, FixedSizeIdProvider, InMemoryBlockIdProvider}
import com.socrata.datacoordinator.util.IdProviderPoolImpl

class TestSqlLoader extends FunSuite with MustMatchers with PropertyChecks with BeforeAndAfterAll {
  val executor = java.util.concurrent.Executors.newCachedThreadPool()

  override def beforeAll() {
    // In Java 6 (sun and open) driver registration is not thread-safe!
    // So since SBT will run these tests in parallel, sometimes one of the
    // first tests to run will randomly fail.  By forcing the driver to
    // be loaded up front we can avoid this.
    Class.forName("org.h2.Driver")
  }

  override def afterAll() {
    executor.shutdownNow()
  }

  def idProvider(initial: Int) = new IdProviderPoolImpl(new InMemoryBlockIdProvider(releasable = false) { override def start = initial }, new FixedSizeIdProvider(_, 1024))

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
          row + (rs.getMetaData.getColumnLabel(col).toUpperCase -> rs.getObject(col))
        }
      }
      result.result()
    }

  def makeTables(conn: Connection, ctx: DataSqlizer[TestColumnType, TestColumnValue]) {
    execute(conn, "drop table if exists " + ctx.dataTableName)
    execute(conn, "drop table if exists " + ctx.logTableName)
    execute(conn, "CREATE TABLE " + ctx.dataTableName + " (id bigint not null primary key," + ctx.datasetContext.userSchema.map { case (c,t) =>
      val sqltype = t match {
        case LongColumn => "BIGINT"
        case StringColumn => "VARCHAR(100)"
      }
      "u_" + c + " " + sqltype + (if(ctx.datasetContext.userPrimaryKeyColumn == Some(c)) " NOT NULL" else " NULL")
    }.mkString(",") + ")")
    ctx.datasetContext.userPrimaryKeyColumn.foreach { pkCol =>
      execute(conn, "CREATE INDEX " + ctx.dataTableName + "_userid ON " + ctx.dataTableName + "(u_" + pkCol + ")")
    }
    // varchar rows because h2 returns a clob for TEXT columns instead of a string
    execute(conn, "CREATE TABLE " + ctx.logTableName + " (version bigint not null, subversion bigint not null, rows varchar(65536) not null, who varchar(100) null, PRIMARY KEY(version, subversion))")
  }

  def preload(conn: Connection, ctx: DataSqlizer[TestColumnType, TestColumnValue])(rows: Map[String, TestColumnValue]*) {
    makeTables(conn, ctx)
    for(row <- rows) {
      val LongValue(id) = row.getOrElse(ctx.datasetContext.systemIdColumnName, sys.error("No :id"))
      val remainingColumns = row - ctx.datasetContext.systemIdColumnName
      assert(remainingColumns.keySet.subsetOf(ctx.datasetContext.userSchema.keySet), "row contains extraneous keys")
      val sql = "insert into " + ctx.dataTableName + " (id," + remainingColumns.keys.map("u_" + _).mkString(",") + ") values (" + id + "," + remainingColumns.values.map(_.sqlize).mkString(",") + ")"
      execute(conn, sql)
    }
  }

  val standardTableName = "test"
  val standardSchema = Map("num" -> LongColumn, "str" -> StringColumn)

  val rowPreparer = new RowPreparer[TestColumnValue] {
    def prepareForInsert(row: Row[TestColumnValue], sid: Long) =
      row + (":id" -> LongValue(sid))

    def prepareForUpdate(row: Row[TestColumnValue]) = row
  }

  test("adding a new row with system PK succeeds") {
    val dsContext = new TestDatasetContext(standardSchema, None)
    val dataSqlizer = new TestDataSqlizer(standardTableName, "hello", dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer)
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, dataSqlizer))
        txn <- managed(SqlLoader(conn, TestTypeContext, rowPreparer, dataSqlizer, dataLogger, idProvider(15), executor))
      } {
        txn.upsert(Map("num" -> LongValue(1), "str" -> StringValue("a")))
        val report = txn.report
        dataLogger.finish()

        report.inserted must equal (Map(0 -> LongValue(15)))
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be ('empty)
        report.elided must be ('empty)
      }
      conn.commit()

      query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq(
        Map("ID" -> 15L, "NUM" -> 1L, "STR" -> "a")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq(
        Map("VERSION" -> 1L, "SUBVERSION" -> 1L, "ROWS" -> """[{"i":{":id":15,"num":1,"str":"a"}}]""", "WHO" -> "hello")
      ))
    }
  }

  test("inserting and then updating a new row with system PK succeeds") {
    val ids = idProvider(15)
    val dsContext = new TestDatasetContext(standardSchema, None)
    val dataSqlizer = new TestDataSqlizer(standardTableName, "hello", dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer)
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, dataSqlizer))
        txn <- managed(SqlLoader(conn, TestTypeContext, rowPreparer, dataSqlizer, dataLogger, ids, executor))
      } {
        txn.upsert(Map("num" -> LongValue(1), "str" -> StringValue("a")))
        txn.upsert(Map(":id" -> LongValue(15), "num" -> LongValue(2), "str" -> StringValue("b")))
        val report = txn.report
        dataLogger.finish()

        report.inserted must equal (Map(0 -> LongValue(15)))
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be ('empty)
        report.elided must equal (Map(1 -> (LongValue(15), 0)))
      }
      conn.commit()

      ids.borrow().allocate() must be (16)

      query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq(Map("ID" -> 15L, "NUM" -> 2L, "STR" -> "b")))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq(
        Map("VERSION" -> 1L, "SUBVERSION" -> 1L, "ROWS" -> """[{"i":{":id":15,"num":2,"str":"b"}}]""", "WHO" -> "hello")
      ))
    }
  }

  test("trying to add a new row with a NULL system PK fails") {
    val dsContext = new TestDatasetContext(standardSchema, None)
    val dataSqlizer = new TestDataSqlizer(standardTableName, "hello", dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer)
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, dataSqlizer))
        txn <- managed(SqlLoader(conn, TestTypeContext, rowPreparer, dataSqlizer, dataLogger, idProvider(22), executor))
      } {
        txn.upsert(Map(":id" -> NullValue, "num" -> LongValue(1), "str" -> StringValue("a")))
        val report = txn.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must equal (Map(0 -> NullPrimaryKey))
        report.elided must be ('empty)
      }
      conn.commit()

      query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq.empty)
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq.empty)
    }
  }

  test("trying to add a new row with a nonexistant system PK fails") {
    val dsContext = new TestDatasetContext(standardSchema, None)
    val dataSqlizer = new TestDataSqlizer(standardTableName, "hello", dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer)
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, dataSqlizer))
        txn <- managed(SqlLoader(conn, TestTypeContext, rowPreparer, dataSqlizer, dataLogger, idProvider(6), executor))
      } {
        txn.upsert(Map(":id" -> LongValue(77), "num" -> LongValue(1), "str" -> StringValue("a")))
        val report = txn.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must equal (Map(0 -> NoSuchRowToUpdate(LongValue(77))))
        report.elided must be ('empty)
      }
      conn.commit()

      query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq.empty)
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq.empty)
    }
  }

  test("updating an existing row by system id succeeds") {
    val ids = idProvider(13)
    val dsContext = new TestDatasetContext(standardSchema, None)
    val dataSqlizer = new TestDataSqlizer(standardTableName, "hello", dsContext)

    withDB() { conn =>
      preload(conn, dataSqlizer)(
        Map(":id" -> LongValue(7), "str" -> StringValue("q"), "num" -> LongValue(2))
      )
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, dataSqlizer))
        txn <- managed(SqlLoader(conn, TestTypeContext, rowPreparer, dataSqlizer, dataLogger, ids, executor))
      } {
        txn.upsert(Map(":id" -> LongValue(7), "num" -> LongValue(44)))
        val report = txn.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must equal (Map(0 -> LongValue(7)))
        report.deleted must be ('empty)
        report.errors must be ('empty)
        report.elided must be ('empty)
      }
      conn.commit()

      ids.borrow().allocate() must be (13)

      query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq(
        Map("ID" -> 7L, "NUM" -> 44L, "STR" -> "q")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq(
        Map("VERSION" -> 1L, "SUBVERSION" -> 1L, "ROWS" -> """[{"u":{"num":44}}]""", "WHO" -> "hello")
      ))
    }
  }

  test("adding a new row with user PK succeeds") {
    val ids = idProvider(15)
    val dsContext = new TestDatasetContext(standardSchema, Some("str"))
    val dataSqlizer = new TestDataSqlizer(standardTableName, "hello", dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer)
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, dataSqlizer))
        txn <- managed(SqlLoader(conn, TestTypeContext, rowPreparer, dataSqlizer, dataLogger, ids, executor))
      } {
        txn.upsert(Map("num" -> LongValue(1), "str" -> StringValue("a")))
        val report = txn.report
        dataLogger.finish()

        report.inserted must equal (Map(0 -> StringValue("a")))
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be ('empty)
        report.elided must be ('empty)
      }
      conn.commit()

      ids.borrow().allocate() must equal (16)

      query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq(
        Map("ID" -> 15L, "NUM" -> 1L, "STR" -> "a")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq(
        Map("VERSION" -> 1L, "SUBVERSION" -> 1L, "ROWS" -> """[{"i":{":id":15,"num":1,"str":"a"}}]""", "WHO" -> "hello")
      ))
    }
  }

  test("trying to add a new row with a NULL user PK fails") {
    val ids = idProvider(22)
    val dsContext = new TestDatasetContext(standardSchema, Some("str"))
    val dataSqlizer = new TestDataSqlizer(standardTableName, "hello", dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer)
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, dataSqlizer))
        txn <- managed(SqlLoader(conn, TestTypeContext, rowPreparer, dataSqlizer, dataLogger, ids, executor))
      } {
        txn.upsert(Map("num" -> LongValue(1), "str" -> NullValue))
        val report = txn.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must equal (Map(0 -> NullPrimaryKey))
        report.elided must be ('empty)
      }
      conn.commit()

      ids.borrow().allocate() must equal (22)

      query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq.empty)
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq.empty)
    }
  }

  test("trying to add a new row without user PK fails") {
    val ids = idProvider(22)
    val dsContext = new TestDatasetContext(standardSchema, Some("str"))
    val dataSqlizer = new TestDataSqlizer(standardTableName, "hello", dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer)
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, dataSqlizer))
        txn <- managed(SqlLoader(conn, TestTypeContext, rowPreparer, dataSqlizer, dataLogger, ids, executor))
      } {
        txn.upsert(Map("num" -> LongValue(1)))
        val report = txn.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be (Map(0 -> NoPrimaryKey))
        report.elided must be ('empty)
      }
      conn.commit()

      ids.borrow().allocate() must equal (22)

      query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq.empty)
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq.empty)
    }
  }

  test("updating an existing row by user pk succeeds") {
    val ids = idProvider(13)
    val dsContext = new TestDatasetContext(standardSchema, Some("str"))
    val dataSqlizer = new TestDataSqlizer(standardTableName, "hello", dsContext)

    withDB() { conn =>
      preload(conn, dataSqlizer)(
        Map(":id" -> LongValue(7), "str" -> StringValue("q"), "num" -> LongValue(2))
      )
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, dataSqlizer))
        txn <- managed(SqlLoader(conn, TestTypeContext, rowPreparer, dataSqlizer, dataLogger, ids, executor))
      } {
        txn.upsert(Map("str" -> StringValue("q"), "num" -> LongValue(44)))
        val report = txn.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must equal (Map(0 -> StringValue("q")))
        report.deleted must be ('empty)
        report.errors must be ('empty)
        report.elided must be ('empty)
      }
      conn.commit()

      ids.borrow().allocate() must be (13)

      query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq(
        Map("ID" -> 7L, "NUM" -> 44L, "STR" -> "q")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq(
        Map("VERSION" -> 1L, "SUBVERSION" -> 1L, "ROWS" -> """[{"u":{"num":44,"str":"q"}}]""", "WHO" -> "hello")
      ))
    }
  }

  test("inserting and then updating a new row with user PK succeeds") {
    val ids = idProvider(15)
    val dsContext = new TestDatasetContext(standardSchema, Some("str"))
    val dataSqlizer = new TestDataSqlizer(standardTableName, "hello", dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer)
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, dataSqlizer))
        txn <- managed(SqlLoader(conn, TestTypeContext, rowPreparer, dataSqlizer, dataLogger, ids, executor))
      } {
        txn.upsert(Map("num" -> LongValue(1), "str" -> StringValue("q")))
        txn.upsert(Map("num" -> LongValue(2), "str" -> StringValue("q")))
        val report = txn.report
        dataLogger.finish()

        report.inserted must equal (Map(0 -> StringValue("q")))
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must be ('empty)
        report.elided must equal (Map(1 -> (StringValue("q"), 0)))
      }
      conn.commit()

      ids.borrow().allocate() must be (16)

      query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq(
        Map("ID" -> 15L, "NUM" -> 2L, "STR" -> "q")
      ))
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq(
        Map("VERSION" -> 1L, "SUBVERSION" -> 1L, "ROWS" -> """[{"i":{":id":15,"num":2,"str":"q"}}]""", "WHO" -> "hello")
      ))
    }
  }

  test("specifying :id when there's a user PK fails") {
    val ids = idProvider(15)
    val dsContext = new TestDatasetContext(standardSchema, Some("str"))
    val dataSqlizer = new TestDataSqlizer(standardTableName, "hello", dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer)
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, dataSqlizer))
        txn <- managed(SqlLoader(conn, TestTypeContext, rowPreparer, dataSqlizer, dataLogger, ids, executor))
      } {
        txn.upsert(Map(":id" -> LongValue(15), "num" -> LongValue(1), "str" -> StringValue("q")))
        val report = txn.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must be ('empty)
        report.errors must equal (Map(0 -> SystemColumnsSet(Set(":id"))))
        report.elided must be ('empty)
      }
      conn.commit()

      ids.borrow().allocate() must be (15)

      query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq.empty)
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq.empty)
    }
  }

  test("deleting a row just inserted with a user PK succeeds") {
    val ids = idProvider(15)
    val dsContext = new TestDatasetContext(standardSchema, Some("str"))
    val dataSqlizer = new TestDataSqlizer(standardTableName, "hello", dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer)
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, dataSqlizer))
        txn <- managed(SqlLoader(conn, TestTypeContext, rowPreparer, dataSqlizer, dataLogger, ids, executor))
      } {
        txn.upsert(Map("num" -> LongValue(1), "str" -> StringValue("q")))
        txn.delete(StringValue("q"))
        val report = txn.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must equal (Map(1 -> StringValue("q")))
        report.elided must equal (Map(0 -> (StringValue("q"), 1)))
        report.errors must be ('empty)
      }
      conn.commit()

      ids.borrow().allocate() must be (15) // and it never even allocated a sid for it

      query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq.empty)
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq.empty)
    }
  }

  test("deleting a row just inserted with a system PK succeeds") {
    // This isn't a useful thing to be able to do, since in the real system
    // IDs won't be user-predictable, but it's a valuable sanity check anyway.
    val ids = idProvider(15)
    val dsContext = new TestDatasetContext(standardSchema, None)
    val dataSqlizer = new TestDataSqlizer(standardTableName, "hello", dsContext)

    withDB() { conn =>
      makeTables(conn, dataSqlizer)
      conn.commit()

      for {
        dataLogger <- managed(new TestDataLogger(conn, dataSqlizer))
        txn <- managed(SqlLoader(conn, TestTypeContext, rowPreparer, dataSqlizer, dataLogger, ids, executor))
      } {
        txn.upsert(Map("num" -> LongValue(1), "str" -> StringValue("q")))
        txn.delete(LongValue(15))
        val report = txn.report
        dataLogger.finish()

        report.inserted must be ('empty)
        report.updated must be ('empty)
        report.deleted must equal (Map(1 -> LongValue(15)))
        report.errors must be ('empty)
        report.elided must equal (Map(0 -> (LongValue(15), 1)))
      }
      conn.commit()

      ids.borrow().allocate() must be (16)

      query(conn, "SELECT id as ID, u_num as NUM, u_str as STR from test_data") must equal (Seq.empty)
      query(conn, "SELECT version, subversion, rows, who from test_log") must equal (Seq.empty)
    }
  }

  test("must contain the same data as a table manipulated by a StupidPostgresTransaction when using user IDs") {
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

    val schema = Map(
      "id" -> LongColumn,
      "num" -> LongColumn,
      "str" -> StringColumn
    )

    val dsContext = new TestDatasetContext(schema, Some("id"))
    val dataSqlizer = new TestDataSqlizer(standardTableName, "hello", dsContext)

    val stupidDsContext = new TestDatasetContext(schema, Some("id"))
    val stupidDataSqlizer = new TestDataSqlizer("stupid_data", "hello", stupidDsContext)

    def applyOps(txn: Loader[TestColumnValue], ops: List[Op]) {
      for(op <- ops) op match {
        case Delete(id) => txn.delete(LongValue(id))
        case Upsert(id, num, data) => txn.upsert(Map(
          "id" -> LongValue(id),
          "num" -> num.map(LongValue(_)).getOrElse(NullValue),
          "str" -> data.map(StringValue(_)).getOrElse(NullValue)
        ))
      }
    }

    forAll { (opss: List[List[Op]]) =>
      withDB() { stupidConn =>
        withDB() { smartConn =>
          makeTables(smartConn, dataSqlizer)
          makeTables(stupidConn, stupidDataSqlizer)

          def runCompareTest(ops: List[Op]) {
            val smartReport = using(SqlLoader(smartConn, TestTypeContext, rowPreparer, dataSqlizer, NullDataLogger, ids, executor)) { txn =>
              applyOps(txn, ops)
              txn.report
            }
            smartConn.commit()

            val stupidReport = using(new StupidSqlLoader(stupidConn, TestTypeContext, rowPreparer, stupidDataSqlizer, NullDataLogger, ids)) { txn =>
              applyOps(txn, ops)
              txn.report
            }
            stupidConn.commit()

            def q(t: String) = "SELECT u_id AS ID, u_num AS NUM, u_str AS STR FROM " + t + " ORDER BY u_id"
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
                val id = smartRs.getLong("ID")
                val num = smartRs.getLong("NUM")
                val numWasNull = smartRs.wasNull()
                val str = smartRs.getString("STR")

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
                val id = stupidRs.getLong("ID")
                val num = stupidRs.getLong("NUM")
                val numWasNull = stupidRs.wasNull()
                val str = stupidRs.getString("STR")

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

  test("must contain the same data as a table manipulated by a StupidPostgresTransaction when using system IDs") {
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

    val dsContext = new TestDatasetContext(standardSchema, None)
    val dataSqlizer = new TestDataSqlizer(standardTableName, "hello", dsContext)

    val stupidDsContext = new TestDatasetContext(standardSchema, None)
    val stupidDataSqlizer = new TestDataSqlizer("stupid_data", "hello", stupidDsContext)

    def applyOps(txn: Loader[TestColumnValue], ops: List[Op]) {
      for(op <- ops) op match {
        case Delete(id) => txn.delete(LongValue(id))
        case Upsert(id, num, data) =>
          val baseRow = Map(
            "num" -> num.map(LongValue(_)).getOrElse(NullValue),
            "str" -> data.map(StringValue(_)).getOrElse(NullValue)
          )
          val row = id match {
            case Some(sid) => baseRow + (":id" -> LongValue(sid))
            case None => baseRow
          }
          txn.upsert(row)
      }
    }

    forAll { (opss: List[List[Op]]) =>
      withDB() { stupidConn =>
        withDB() { smartConn =>
          makeTables(smartConn, dataSqlizer)
          makeTables(stupidConn, stupidDataSqlizer)

          val smartIds = idProvider(1)
          val stupidIds = idProvider(1)

          def runCompareTest(ops: List[Op]) {
            val smartReport =
              using(SqlLoader(smartConn, TestTypeContext, rowPreparer, dataSqlizer, NullDataLogger, smartIds, executor)) { txn =>
                applyOps(txn, ops)
                txn.report
              }
            smartConn.commit()

            val stupidReport =
              using(new StupidSqlLoader(stupidConn, TestTypeContext, rowPreparer, stupidDataSqlizer, NullDataLogger, stupidIds)) { txn =>
                applyOps(txn, ops)
                txn.report
              }
            stupidConn.commit()

            def q(t: String) = "SELECT u_num AS NUM, u_str AS STR FROM " + t + " ORDER BY u_num, u_str"
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
                val num = smartRs.getLong("NUM")
                val numWasNull = smartRs.wasNull()
                val str = smartRs.getString("STR")

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
                val num = stupidRs.getLong("NUM")
                val numWasNull = stupidRs.wasNull()
                val str = stupidRs.getString("STR")

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
