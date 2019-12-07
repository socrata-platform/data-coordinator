package com.socrata.datacoordinator
package truth.loader
package sql

import java.lang.StringBuilder
import java.io.Writer
import java.sql.{ResultSet, Connection}

import com.rojoma.simplearm.v2._

import com.socrata.datacoordinator.id.ColumnId
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.sql.{SqlColumnReadRep, SqlColumnRep}

class RepBasedDatasetCsvifier[CT, CV](conn: Connection, dataTableName: String, schema: ColumnIdMap[SqlColumnRep[CT, CV]], nullValue: CV)
  extends DatasetCsvifier[CV]
{
  private def csvifyRow(target: StringBuilder, rs: ResultSet, reps: Array[SqlColumnRep[CT, CV]]): StringBuilder = {
    var src = 1
    var idx = 0

    var didOne = false
    while(idx < reps.length) {
      if(didOne) target.append(',')
      else didOne = true

      val rep = reps(idx)
      val v = rep.fromResultSet(rs, src)
      rep.csvifyForInsert(target, v)

      idx += 1
      src += rep.physColumns.length
    }
    target.append('\n')
  }

  def csvify(target: Writer, columns: Seq[ColumnId]) {
    if(columns.nonEmpty) {
      val orderedReps = columns.map(schema(_)).toArray
      val colSelectors = orderedReps.flatMap(_.physColumns)
      val q = "SELECT " + colSelectors.mkString(",") + " FROM " + dataTableName
      using(conn.createStatement()) { stmt =>
        stmt.setFetchSize(1000)
        using(stmt.executeQuery(q)) { rs =>
          val sb = new StringBuilder
          while(rs.next()) {
            target.append(csvifyRow(sb, rs, orderedReps))
            sb.setLength(0)
          }
        }
      }
    }
  }
}
