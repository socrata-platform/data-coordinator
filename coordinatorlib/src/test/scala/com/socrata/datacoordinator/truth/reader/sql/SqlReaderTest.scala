package com.socrata.datacoordinator
package truth.reader.sql

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.matchers.MustMatchers
import com.rojoma.simplearm.util._

import truth.{RowIdMap, DatasetContext}
import java.sql.{DriverManager, Connection}
import truth.sql.SqlColumnReadRep
import util.LongLikeMap

class SqlReaderTest extends FunSuite with MustMatchers with BeforeAndAfterAll {
  override def beforeAll() {
    // In Java 6 (sun and open) driver registration is not thread-safe!
    // So since SBT will run these tests in parallel, sometimes one of the
    // first tests to run will randomly fail.  By forcing the driver to
    // be loaded up front we can avoid this.
    Class.forName("org.h2.Driver")
  }

  def datasetContext(schema: LongLikeMap[ColumnId, TestColumnType]) = new DatasetContext[TestColumnType, TestColumnValue] {
    def hasCopy = false

    val userSchema = schema

    val userPrimaryKeyColumn = if(schema.contains(100L)) Some(100L) else None

    def userPrimaryKey(row: Row[TestColumnValue]) = row.get(userPrimaryKeyColumn.get)

    def systemId(row: Row[TestColumnValue]) = row.get(0L).map(_.asInstanceOf[IdValue].value)

    def systemIdAsValue(row: Row[TestColumnValue]) = row.get(0L)

    def systemColumns(row: Row[TestColumnValue]) = row.keySet.filter(_ != 0L).toSet

    val systemSchema = LongLikeMap[ColumnId, TestColumnType](0L -> IdType)

    def systemIdColumnName = 0L

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

  def repSchemaBuilder(schema: LongLikeMap[ColumnId, TestColumnType]): LongLikeMap[ColumnId, SqlColumnReadRep[TestColumnType, TestColumnValue]] = {
    val res = new LongLikeMap[ColumnId, SqlColumnReadRep[TestColumnType, TestColumnValue]]
    for((k, v) <- schema.iterator) {
      res += k -> (v match {
        case IdType => new IdRep(0L)
        case NumberType => new NumberRep(k)
        case StringType => new StringRep(k)
      })
    }
    res
  }

  val tableName = "tab"

  def create(conn: Connection, schema: LongLikeMap[ColumnId, TestColumnType]) {
    val sb = new StringBuilder("CREATE TABLE ").append(tableName).append(" (c_0 BIGINT NOT NULL PRIMARY KEY")
    for((k, v) <- schema.iterator) {
      sb.append(",").append("c_" + k).append(" ")
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

  def load(conn: Connection)(rows: Seq[(Long, TestColumnValue)]*) {
    using(conn.createStatement()) { stmt =>
      for(row <- rows) {
        val sb = new StringBuilder("INSERT INTO ").append(tableName).append(" (").append(row.iterator.map(c => "c_" + c._1).mkString(",")).append(") VALUES (")
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
    val schema = LongLikeMap[ColumnId, TestColumnType](
      1L -> StringType,
      2L -> NumberType
    )
    create(conn, schema)
    load(conn)(
      List(0L -> IdValue(1), 1L -> StringValue("a"), 2L -> NumberValue(1000)),
      List(0L -> IdValue(2), 1L -> NullValue, 2L -> NumberValue(1001)),
      List(0L -> IdValue(3), 1L -> StringValue("b"), 2L -> NullValue),
      List(0L -> IdValue(4), 1L -> NullValue, 2L -> NullValue)
    )

    managed(new SqlReader(conn, tableName, datasetContext(schema), TestTypeContext, repSchemaBuilder, blockSize = 3))
  }

  def stdUidTable(conn: Connection) = {
    val schema = LongLikeMap[ColumnId, TestColumnType](
      100L -> StringType,
      1L -> StringType,
      2L -> NumberType
    )
    create(conn, schema)
    load(conn)(
      List(0L -> IdValue(1), 100L -> StringValue("alpha"), 1L -> StringValue("a"), 2L -> NumberValue(1000)),
      List(0L -> IdValue(2), 100L -> StringValue("beta"), 1L -> NullValue, 2L -> NumberValue(1001)),
      List(0L -> IdValue(3), 100L -> StringValue("gamma"), 1L -> StringValue("b"), 2L -> NullValue),
      List(0L -> IdValue(4), 100L -> StringValue("delta"), 1L -> NullValue, 2L -> NullValue)
    )

    managed(new SqlReader(conn, tableName, datasetContext(schema), TestTypeContext, repSchemaBuilder, blockSize = 3))
  }

  test("Can read rows that exist from a table by system id") {
    for {
      conn <- managedConn
      r <- stdSidTable(conn)
      rows <- managed(r.lookupBySystemId(Set(1L), Iterator(4,3,2,1)))
    } {
      rows.flatten.toList must equal (List(
        4 -> Some(LongLikeMap(1L -> NullValue)),
        3 -> Some(LongLikeMap(1L -> StringValue("b"))),
        2 -> Some(LongLikeMap(1L -> NullValue)),
        1 -> Some(LongLikeMap(1L -> StringValue("a")))
      ))
    }
  }

  test("Can read multiple columns by system id") {
    for {
      conn <- managedConn
      r <- stdSidTable(conn)
      rows <- managed(r.lookupBySystemId(Set(1L, 2L), Iterator(2)))
    } {
      rows.flatten.toList must equal (List(
        2 -> Some(LongLikeMap(1L -> NullValue, 2L -> NumberValue(1001)))
      ))
    }
  }

  test("Can read rows that do not exist from a table by system id") {
    for {
      conn <- managedConn
      r <- stdSidTable(conn)
      rows <- managed(r.lookupBySystemId(Set(1L), Iterator(4,99,98,1)))
    } {
      rows.flatten.toList must equal (List(
        4 -> Some(LongLikeMap(1L -> NullValue)),
        99 -> None,
        98 -> None,
        1 -> Some(LongLikeMap(1L -> StringValue("a")))
      ))
    }
  }

  test("Can read rows that exist from a table by user id") {
    for {
      conn <- managedConn
      r <- stdUidTable(conn)
      rows <- managed(r.lookupByUserId(Set(1L), Iterator("delta", "gamma", "beta", "alpha").map(StringValue(_))))
    } {
      rows.flatten.toList must equal (List(
        StringValue("delta") -> Some(LongLikeMap(1L -> NullValue)),
        StringValue("gamma") -> Some(LongLikeMap(1L -> StringValue("b"))),
        StringValue("beta") -> Some(LongLikeMap(1L -> NullValue)),
        StringValue("alpha") -> Some(LongLikeMap(1L -> StringValue("a")))
      ))
    }
  }

  test("Can read multiple columns by user id") {
    for {
      conn <- managedConn
      r <- stdUidTable(conn)
      rows <- managed(r.lookupByUserId(Set(1L,2L), Iterator(StringValue("beta"))))
    } {
      rows.flatten.toList must equal (List(
        StringValue("beta") -> Some(LongLikeMap(1L -> NullValue, 2L -> NumberValue(1001)))
      ))
    }
  }

  test("Can read rows that do not exist from a table by user id") {
    for {
      conn <- managedConn
      r <- stdUidTable(conn)
      rows <- managed(r.lookupByUserId(Set(1L), Iterator("delta", "psi", "omega", "alpha").map(StringValue(_))))
    } {
      rows.flatten.toList must equal (List(
        StringValue("delta") -> Some(LongLikeMap(1L -> NullValue)),
        StringValue("psi") -> None,
        StringValue("omega") -> None,
        StringValue("alpha") -> Some(LongLikeMap(1L -> StringValue("a")))
      ))
    }
  }
}
