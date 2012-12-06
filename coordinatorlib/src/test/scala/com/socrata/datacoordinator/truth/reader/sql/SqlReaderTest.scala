package com.socrata.datacoordinator
package truth.reader.sql

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.matchers.MustMatchers
import com.rojoma.simplearm.util._

import truth.{RowIdMap, DatasetContext}
import java.sql.{DriverManager, Connection}
import truth.sql.SqlColumnReadRep

class SqlReaderTest extends FunSuite with MustMatchers with BeforeAndAfterAll {
  override def beforeAll() {
    // In Java 6 (sun and open) driver registration is not thread-safe!
    // So since SBT will run these tests in parallel, sometimes one of the
    // first tests to run will randomly fail.  By forcing the driver to
    // be loaded up front we can avoid this.
    Class.forName("org.h2.Driver")
  }

  def datasetContext(schema: Map[String, TestColumnType]) = new DatasetContext[TestColumnType, TestColumnValue] {
    def hasCopy = false

    val userSchema = schema

    val userPrimaryKeyColumn = if(schema.contains("pk")) Some("pk") else None

    def userPrimaryKey(row: Row[TestColumnValue]) = row.get(userPrimaryKeyColumn.get)

    def systemId(row: Row[TestColumnValue]) = row.get(":id").map(_.asInstanceOf[IdValue].value)

    def systemIdAsValue(row: Row[TestColumnValue]) = row.get(":id")

    def systemColumns(row: Row[TestColumnValue]) = row.keySet.filter(_.startsWith(":"))

    val systemSchema = Map(":id" -> IdType)

    def systemIdColumnName = ":id"

    val fullSchema = userSchema ++ systemSchema

    def makeIdMap[T]() = new RowIdMap[TestColumnValue, T] {
      val underlying = new scala.collection.mutable.HashMap[String, T]

      def s(x: TestColumnValue) = x.asInstanceOf[StringValue].value

      def put(x: TestColumnValue, v: T) {
        underlying += s(x) -> v
      }

      def apply(x: TestColumnValue) = underlying(s(x))

      def get(x: TestColumnValue) = underlying.get(s(x))

      def clear() { underlying.clear() }

      def contains(x: TestColumnValue) = underlying.contains(s(x))

      def isEmpty = underlying.isEmpty

      def size = underlying.size

      def foreach(f: (TestColumnValue, T) => Unit) {
        underlying.foreach { case (k,v) =>
          f(StringValue(k), v)
        }
      }

      def valuesIterator = underlying.valuesIterator
    }

    def mergeRows(base: Row[TestColumnValue], overlay: Row[TestColumnValue]) = sys.error("Shouldn't call this")
  }

  def managedConn = managed(DriverManager.getConnection("jdbc:h2:mem:"))

  def repSchemaBuilder(schema: Map[String, TestColumnType]): Map[String, SqlColumnReadRep[TestColumnType, TestColumnValue]] = {
    schema.map { case (k, v) =>
      k -> (v match {
        case IdType => new IdRep("id")
        case NumberType => new NumberRep(k)
        case StringType => new StringRep(k)
      })
    }
  }

  val tableName = "tab"

  def create(conn: Connection, schema: Map[String, TestColumnType]) {
    val sb = new StringBuilder("CREATE TABLE ").append(tableName).append(" (id BIGINT NOT NULL PRIMARY KEY")
    for((k, v) <- schema) {
      sb.append(",").append(k).append(" ")
      val typ = v match {
        case StringType => "VARCHAR(255)"
        case NumberType => "BIGINT"
        case IdType => sys.error("shouldn't have IdType here")
      }
      sb.append(typ)
    }
    sb.append(")")

    using(conn.createStatement()) { stmt =>
      stmt.execute(sb.toString)
    }
  }

  def load(conn: Connection)(rows: Seq[(String, TestColumnValue)]*) {
    using(conn.createStatement()) { stmt =>
      for(row <- rows) {
        val sb = new StringBuilder("INSERT INTO ").append(tableName).append(" (").append(row.map(_._1).mkString(",")).append(") VALUES (")
        val vals = row.map(_._2).map {
          case IdValue(v) => v
          case NumberValue(v) => v
          case StringValue(v) => "'" + v.replaceAllLiterally("'","''") + "'"
          case NullValue => "null"
        }
        sb.append(vals.mkString(",")).append(")")
        stmt.execute(sb.toString)
      }
    }
  }

  def stdSidTable(conn: Connection) = {
    val schema = Map(
      "str" -> StringType,
      "num" -> NumberType
    )
    create(conn, schema)
    load(conn)(
      List("id" -> IdValue(1), "str" -> StringValue("a"), "num" -> NumberValue(1000)),
      List("id" -> IdValue(2), "str" -> NullValue, "num" -> NumberValue(1001)),
      List("id" -> IdValue(3), "str" -> StringValue("b"), "num" -> NullValue),
      List("id" -> IdValue(4), "str" -> NullValue, "num" -> NullValue)
    )

    managed(new SqlReader(conn, tableName, datasetContext(schema), TestTypeContext, repSchemaBuilder, blockSize = 3))
  }

  def stdUidTable(conn: Connection) = {
    val schema = Map(
      "pk" -> StringType,
      "str" -> StringType,
      "num" -> NumberType
    )
    create(conn, schema)
    load(conn)(
      List("id" -> IdValue(1), "pk" -> StringValue("alpha"), "str" -> StringValue("a"), "num" -> NumberValue(1000)),
      List("id" -> IdValue(2), "pk" -> StringValue("beta"), "str" -> NullValue, "num" -> NumberValue(1001)),
      List("id" -> IdValue(3), "pk" -> StringValue("gamma"), "str" -> StringValue("b"), "num" -> NullValue),
      List("id" -> IdValue(4), "pk" -> StringValue("delta"), "str" -> NullValue, "num" -> NullValue)
    )

    managed(new SqlReader(conn, tableName, datasetContext(schema), TestTypeContext, repSchemaBuilder, blockSize = 3))
  }

  test("Can read rows that exist from a table by system id") {
    for {
      conn <- managedConn
      r <- stdSidTable(conn)
      rows <- managed(r.lookupBySystemId(Set("str"), Iterator(4,3,2,1)))
    } {
      rows.flatten.toList must equal (List(
        4 -> Some(Map("str" -> NullValue)),
        3 -> Some(Map("str" -> StringValue("b"))),
        2 -> Some(Map("str" -> NullValue)),
        1 -> Some(Map("str" -> StringValue("a")))
      ))
    }
  }

  test("Can read multiple columns by system id") {
    for {
      conn <- managedConn
      r <- stdSidTable(conn)
      rows <- managed(r.lookupBySystemId(Set("str","num"), Iterator(2)))
    } {
      rows.flatten.toList must equal (List(
        2 -> Some(Map("str" -> NullValue, "num" -> NumberValue(1001)))
      ))
    }
  }

  test("Can read rows that do not exist from a table by system id") {
    for {
      conn <- managedConn
      r <- stdSidTable(conn)
      rows <- managed(r.lookupBySystemId(Set("str"), Iterator(4,99,98,1)))
    } {
      rows.flatten.toList must equal (List(
        4 -> Some(Map("str" -> NullValue)),
        99 -> None,
        98 -> None,
        1 -> Some(Map("str" -> StringValue("a")))
      ))
    }
  }

  test("Can read rows that exist from a table by user id") {
    for {
      conn <- managedConn
      r <- stdUidTable(conn)
      rows <- managed(r.lookupByUserId(Set("str"), Iterator("delta", "gamma", "beta", "alpha").map(StringValue(_))))
    } {
      rows.flatten.toList must equal (List(
        StringValue("delta") -> Some(Map("str" -> NullValue)),
        StringValue("gamma") -> Some(Map("str" -> StringValue("b"))),
        StringValue("beta") -> Some(Map("str" -> NullValue)),
        StringValue("alpha") -> Some(Map("str" -> StringValue("a")))
      ))
    }
  }

  test("Can read multiple columns by user id") {
    for {
      conn <- managedConn
      r <- stdUidTable(conn)
      rows <- managed(r.lookupByUserId(Set("str","num"), Iterator(StringValue("beta"))))
    } {
      rows.flatten.toList must equal (List(
        StringValue("beta") -> Some(Map("str" -> NullValue, "num" -> NumberValue(1001)))
      ))
    }
  }

  test("Can read rows that do not exist from a table by user id") {
    for {
      conn <- managedConn
      r <- stdUidTable(conn)
      rows <- managed(r.lookupByUserId(Set("str"), Iterator("delta", "psi", "omega", "alpha").map(StringValue(_))))
    } {
      rows.flatten.toList must equal (List(
        StringValue("delta") -> Some(Map("str" -> NullValue)),
        StringValue("psi") -> None,
        StringValue("omega") -> None,
        StringValue("alpha") -> Some(Map("str" -> StringValue("a")))
      ))
    }
  }
}
