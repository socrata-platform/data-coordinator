package com.socrata.datacoordinator.truth
package sql

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.metadata.{DatasetMapReader, CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.util.collection.{MutableColumnIdMap, ColumnIdMap}
import com.socrata.datacoordinator.id.ColumnId
import javax.sql.DataSource
import java.sql.{ResultSet, Connection}

// Does this need to be *Postgres*, or is all postgres-specific stuff encapsulated in its paramters?
class PostgresMonadicDatabaseReader[CT, CV](dataSource: DataSource,
                                            mapReaderFactory: Connection => DatasetMapReader,
                                            repFor: ColumnInfo => SqlColumnReadRep[CT, CV])
  extends LowLevelMonadicDatabaseReader[CV]
{
  private class S(conn: Connection) extends ReadContext {
    val datasetMap: DatasetMapReader = mapReaderFactory(conn)

    def withRows[A](ci: CopyInfo, schema: ColumnIdMap[ColumnInfo], f: (Iterator[ColumnIdMap[CV]]) => A): A = {
      val reps = schema.values.map(repFor).toArray
      val cids = schema.values.map(_.systemId.underlying).toArray
      val q = "SELECT " + reps.flatMap(_.physColumns).mkString(",") + " FROM " + ci.dataTableName
      using(conn.createStatement()) { stmt =>
        stmt.setFetchSize(1000)
        using(stmt.executeQuery(q)) { rs =>
          f(rsToIterator(rs, cids, reps))
        }
      }
    }

    def rsToIterator(rs: ResultSet, cids: Array[Long], reps: Array[SqlColumnReadRep[CT, CV]]): Iterator[ColumnIdMap[CV]] = {
      def loop(): Stream[ColumnIdMap[CV]] = {
        if(rs.next()) {
          toRow(rs, cids, reps) #:: loop()
        } else {
          Stream.empty
        }
      }
      loop().iterator
    }

    def toRow(rs: ResultSet, cids: Array[Long], reps: Array[SqlColumnReadRep[CT, CV]]): ColumnIdMap[CV] = {
      val row = new MutableColumnIdMap[CV]
      var i = 0
      var src = 1
      while(i != reps.length) {
        row(new ColumnId(cids(i))) = reps(i).fromResultSet(rs, src)
        src += reps(i).physColumns.length
        i += 1
      }
      row.freeze()
    }
  }

  def runTransaction[A](f: ReadContext => A): A =
    using(dataSource.getConnection()) { conn =>
      conn.setAutoCommit(false)
      conn.setReadOnly(true)
      f(new S(conn))
    }
}
