package com.socrata.datacoordinator.truth
package sql

import scalaz._
import scalaz.effect._
import Scalaz._

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.metadata.{DatasetMapReader, CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.util.collection.{MutableColumnIdMap, ColumnIdMap}
import com.socrata.datacoordinator.id.ColumnId
import javax.sql.DataSource
import java.sql.{ResultSet, Connection}

// Does this need to be *Postgres*, or is all postgres-specific stuff encapsulated in its paramters?
class PostgresMonadicDatabaseReader[CT, CV](ds: DataSource,
                                            mapReaderFactory: Connection => DatasetMapReader,
                                            repFor: ColumnInfo => SqlColumnReadRep[CT, CV])
  extends DatabaseIO(ds) with LowLevelMonadicDatabaseReader[CV]
{
  import Kleisli.ask

  case class S(conn: Connection, datasetMap: DatasetMapReader)
  type ReadContext = S

  def createInitialState(conn: Connection): IO[S] = IO {
    S(conn, mapReaderFactory(conn))
  }

  def runTransaction[A](action: DatabaseM[A]): IO[A] =
    withConnection { conn =>
      for {
        _ <- IO { conn.setAutoCommit(false); conn.setReadOnly(true) }
        initialState <- createInitialState(conn)
        result <- action.run(initialState)
      } yield result
    }

  val get: DatabaseM[S] = ask

  val connRaw = get.map(_.conn)
  val datasetMap: DatabaseM[DatasetMapReader] = get.map(_.datasetMap)

  def io[A](op: => A): DatabaseM[A] = IO(op).liftKleisli[S]

  def withRows[A](ci: CopyInfo, schema: ColumnIdMap[ColumnInfo], f: Iterator[ColumnIdMap[CV]] => IO[A]): DatabaseM[A] =
    connRaw.flatMap { conn =>
      io {
        val reps = schema.values.map(repFor).toArray
        val cids = schema.values.map(_.systemId.underlying).toArray
        val q = "SELECT " + reps.flatMap(_.physColumns).mkString(",") + " FROM " + ci.dataTableName
        using(conn.createStatement()) { stmt =>
          stmt.setFetchSize(1000)
          using(stmt.executeQuery(q)) { rs =>
            f(rsToIterator(rs, cids, reps)).unsafePerformIO()
          }
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
