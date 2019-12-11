package com.socrata.datacoordinator
package truth.loader
package sql

import com.rojoma.simplearm.v2._

import java.sql.{ResultSet, Connection}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.sql.{SqlPKableColumnReadRep, SqlColumnReadRep}
import com.socrata.datacoordinator.id.ColumnId

class RepBasedDatasetExtractor[CT, CV](conn: Connection, dataTableName: String, sidCol: SqlPKableColumnReadRep[CT, CV], schema: ColumnIdMap[SqlColumnReadRep[CT, CV]])
  extends DatasetExtractor[CV]
{
  require(!conn.getAutoCommit, "Connection must not be in auto-commit mode")

  val cids = schema.keys.map(_.underlying).toArray
  val reps = cids.map { cid => schema(new ColumnId(cid)) }

  private def rowify(rs: ResultSet): Row[CV] = {
    val result = new MutableRow[CV]
    var src = 1
    var idx = 0
    while(idx != cids.length) {
      val rep = reps(idx)
      result(new ColumnId(cids(idx))) = rep.fromResultSet(rs, src)
      src += rep.physColumns.length
      idx += 1
    }
    result.freeze()
  }

  def allRows(limit: Option[Long], offset: Option[Long], sorted: Boolean, rowId: Option[CV])
    : Managed[Iterator[Row[CV]]] = new Managed[Iterator[Row[CV]]] {
    def run[B](f: (Iterator[Row[CV]]) => B): B = {
      if(schema.isEmpty) {
        f(Iterator.empty)
      } else {
        val colSelectors = cids.map { cid => schema(new ColumnId(cid)).selectList }
        val q = "SELECT " + colSelectors.mkString(",") + " FROM " + dataTableName +
          (if (rowId.isDefined) " WHERE " + sidCol.templateForSingleLookup else "" ) +
          (if(sorted) " ORDER BY " + sidCol.orderBy() else "") +
          limit.map { l => " LIMIT " + l.max(0) }.getOrElse("") +
          offset.map { o => " OFFSET " + o.max(0) }.getOrElse("")
        using(conn.prepareStatement(q)) { stmt =>
          rowId.foreach(v => sidCol.prepareSingleLookup(stmt, v, 1))
          stmt.setFetchSize(1000)
          using(stmt.executeQuery()) { rs =>
            def loop(): Stream[Row[CV]] =
              if(rs.next()) {
                rowify(rs) #:: loop()
              } else {
                Stream.empty
              }

            f(loop().iterator)
          }
        }
      }
    }
  }
}
