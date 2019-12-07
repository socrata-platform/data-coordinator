package com.socrata.datacoordinator
package truth.reader.sql

import java.sql.{DriverManager, Connection}

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.MustMatchers
import com.rojoma.simplearm.v2._

import com.socrata.datacoordinator.truth.sql.{ReadOnlyRepBasedSqlDatasetContext, SqlColumnReadRep}
import com.socrata.datacoordinator.util.collection.{ColumnIdSet, MutableColumnIdMap, ColumnIdMap}
import com.socrata.datacoordinator.id.{RowId, ColumnId}

class SqlReaderTest extends FunSuite with MustMatchers with BeforeAndAfterAll {

  Class.forName("org.h2.Driver") // force driver to load

  def datasetContext(s: ColumnIdMap[SqlColumnReadRep[TestColumnType, TestColumnValue]]) = new ReadOnlyRepBasedSqlDatasetContext[TestColumnType, TestColumnValue] {
    val schema = s
    val typeContext = TestTypeContext

    val userPrimaryKeyColumn = if(schema.contains(new ColumnId(100L))) Some(new ColumnId(100L)) else None
    val userPrimaryKeyType = userPrimaryKeyColumn.map(_ => StringType)

    val systemIdColumn = new ColumnId(0L)
    val versionColumn = new ColumnId(1L)

    val systemColumnIds = ColumnIdSet(systemIdColumn)

    def mergeRows(base: Row[TestColumnValue], overlay: Row[TestColumnValue]) = sys.error("Shouldn't call this")

    val primaryKeyColumn: ColumnId = userPrimaryKeyColumn.getOrElse(systemIdColumn)
    val primaryKeyType: TestColumnType = schema(primaryKeyColumn).representedType
  }

  def managedConn = managed(DriverManager.getConnection("jdbc:h2:mem:"))

  def repSchemaBuilder(schema: ColumnIdMap[TestColumnType]): ColumnIdMap[SqlColumnReadRep[TestColumnType, TestColumnValue]] = {
    val res = new MutableColumnIdMap[SqlColumnReadRep[TestColumnType, TestColumnValue]]
    schema.foreach { (col, typ) =>
      res(col) = typ match {
        case IdType => sys.error("Shouldn't have an ID col at this point")
        case NumberType => new NumberRep(col)
        case StringType => new StringRep(col)
      }
    }
    res(new ColumnId(0)) = new IdRep(new ColumnId(0))
    res.freeze()
  }

  val tableName = "tab"

  def create(conn: Connection, schema: ColumnIdMap[TestColumnType]) {
    val sb = new StringBuilder("CREATE TABLE ").append(tableName).append(" (c_0 BIGINT NOT NULL PRIMARY KEY")
    for((k, v) <- schema.iterator) {
      sb.append(",").append("c_" + k.underlying).append(" ")
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
          case IdValue(v) => v.underlying
          case VersionValue(v) => v.underlying
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
    val schema = ColumnIdMap[TestColumnType](
      new ColumnId(1L) -> StringType,
      new ColumnId(2L) -> NumberType
    )
    create(conn, schema)
    load(conn)(
      List(0L -> IdValue(new RowId(1)), 1L -> StringValue("a"), 2L -> NumberValue(1000)),
      List(0L -> IdValue(new RowId(2)), 1L -> NullValue, 2L -> NumberValue(1001)),
      List(0L -> IdValue(new RowId(3)), 1L -> StringValue("b"), 2L -> NullValue),
      List(0L -> IdValue(new RowId(4)), 1L -> NullValue, 2L -> NullValue)
    )

    managed(new SqlReader(conn, tableName, datasetContext(repSchemaBuilder(schema)), TestTypeContext, blockSize = 3))
  }

  def stdUidTable(conn: Connection) = {
    val schema = ColumnIdMap[TestColumnType](
      new ColumnId(100L) -> StringType,
      new ColumnId(1L) -> StringType,
      new ColumnId(2L) -> NumberType
    )
    create(conn, schema)
    load(conn)(
      List(0L -> IdValue(new RowId(1)), 100L -> StringValue("alpha"), 1L -> StringValue("a"), 2L -> NumberValue(1000)),
      List(0L -> IdValue(new RowId(2)), 100L -> StringValue("beta"), 1L -> NullValue, 2L -> NumberValue(1001)),
      List(0L -> IdValue(new RowId(3)), 100L -> StringValue("gamma"), 1L -> StringValue("b"), 2L -> NullValue),
      List(0L -> IdValue(new RowId(4)), 100L -> StringValue("delta"), 1L -> NullValue, 2L -> NullValue)
    )

    managed(new SqlReader(conn, tableName, datasetContext(repSchemaBuilder(schema)), TestTypeContext, blockSize = 3))
  }

  test("Can read rows that exist from a table by system id") {
    val one = new ColumnId(1)
    for {
      conn <- managedConn
      r <- stdSidTable(conn)
      rows <- managed(r.lookupBySystemId(Set(one), Iterator(4,3,2,1).map(new RowId(_))))
    } {
      rows.flatten.toList must equal (List(
        new RowId(4) -> Some(ColumnIdMap(one -> NullValue)),
        new RowId(3) -> Some(ColumnIdMap(one -> StringValue("b"))),
        new RowId(2) -> Some(ColumnIdMap(one -> NullValue)),
        new RowId(1) -> Some(ColumnIdMap(one -> StringValue("a")))
      ))
    }
  }

  test("Can read multiple columns by system id") {
    for {
      conn <- managedConn
      r <- stdSidTable(conn)
      rows <- managed(r.lookupBySystemId(Set(new ColumnId(1L), new ColumnId(2L)), Iterator(new RowId(2))))
    } {
      rows.flatten.toList must equal (List(
        new RowId(2) -> Some(ColumnIdMap(new ColumnId(1L) -> NullValue, new ColumnId(2L) -> NumberValue(1001)))
      ))
    }
  }

  test("Can read rows that do not exist from a table by system id") {
    for {
      conn <- managedConn
      r <- stdSidTable(conn)
      rows <- managed(r.lookupBySystemId(Set(new ColumnId(1L)), Iterator(4,99,98,1).map(new RowId(_))))
    } {
      rows.flatten.toList must equal (List(
        new RowId(4) -> Some(ColumnIdMap(new ColumnId(1L) -> NullValue)),
        new RowId(99) -> None,
        new RowId(98) -> None,
        new RowId(1) -> Some(ColumnIdMap(new ColumnId(1L) -> StringValue("a")))
      ))
    }
  }

  test("Can read rows that exist from a table by user id") {
    for {
      conn <- managedConn
      r <- stdUidTable(conn)
      rows <- managed(r.lookupByUserId(Set(new ColumnId(1L)), Iterator("delta", "gamma", "beta", "alpha").map(StringValue(_))))
    } {
      rows.flatten.toList must equal (List(
        StringValue("delta") -> Some(ColumnIdMap(new ColumnId(1L) -> NullValue)),
        StringValue("gamma") -> Some(ColumnIdMap(new ColumnId(1L) -> StringValue("b"))),
        StringValue("beta") -> Some(ColumnIdMap(new ColumnId(1L) -> NullValue)),
        StringValue("alpha") -> Some(ColumnIdMap(new ColumnId(1L) -> StringValue("a")))
      ))
    }
  }

  test("Can read multiple columns by user id") {
    for {
      conn <- managedConn
      r <- stdUidTable(conn)
      rows <- managed(r.lookupByUserId(Set(new ColumnId(1L),new ColumnId(2L)), Iterator(StringValue("beta"))))
    } {
      rows.flatten.toList must equal (List(
        StringValue("beta") -> Some(ColumnIdMap(new ColumnId(1L) -> NullValue, new ColumnId(2L) -> NumberValue(1001)))
      ))
    }
  }

  test("Can read rows that do not exist from a table by user id") {
    for {
      conn <- managedConn
      r <- stdUidTable(conn)
      rows <- managed(r.lookupByUserId(Set(new ColumnId(1L)), Iterator("delta", "psi", "omega", "alpha").map(StringValue(_))))
    } {
      rows.flatten.toList must equal (List(
        StringValue("delta") -> Some(ColumnIdMap(new ColumnId(1L) -> NullValue)),
        StringValue("psi") -> None,
        StringValue("omega") -> None,
        StringValue("alpha") -> Some(ColumnIdMap(new ColumnId(1L) -> StringValue("a")))
      ))
    }
  }
}
