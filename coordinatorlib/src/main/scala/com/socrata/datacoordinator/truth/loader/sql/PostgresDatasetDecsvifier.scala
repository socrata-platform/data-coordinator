package com.socrata.datacoordinator.truth.loader
package sql

import java.sql.Connection
import java.io.{OutputStream, Reader}

import com.socrata.datacoordinator.id.ColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnReadRep
import com.socrata.datacoordinator.util.collection.ColumnIdMap

class PostgresDatasetDecsvifier[CT, CV](conn: Connection, copyIn: (Connection, String, OutputStream => Unit) => Long, dataTableName: String, schema: ColumnIdMap[SqlColumnReadRep[CT, CV]])
  extends DatasetDecsvifier
{
  def importFromCsv(output: OutputStream => Unit, columns: Seq[ColumnId]) {
    val physColumns = columns.flatMap(schema(_).physColumns)
    val sql = "COPY " + dataTableName + " (" + physColumns.mkString(",") + ") from stdin with csv"
    copyIn(conn, sql, output)
  }
}
