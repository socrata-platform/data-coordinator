package com.socrata.datacoordinator.truth.loader
package sql

import java.sql.Connection
import org.postgresql.copy.CopyManager
import java.io.Reader
import com.socrata.datacoordinator.id.ColumnId
import com.socrata.datacoordinator.truth.sql.{SqlColumnReadRep, RepBasedSqlDatasetContext}
import com.socrata.datacoordinator.util.collection.ColumnIdMap

class PostgresDatasetDecsvifier[CT, CV](conn: Connection, extractCopier: Connection => CopyManager, dataTableName: String, schema: ColumnIdMap[SqlColumnReadRep[CT, CV]])
  extends DatasetDecsvifier[CV]
{
  def importFromCsv(reader: Reader, columns: Seq[ColumnId]) {
    val physColumns = columns.flatMap(schema(_).physColumns)
    val sql = "COPY " + dataTableName + " (" + physColumns.mkString(",") + ") from stdin with csv"
    extractCopier(conn).copyIn(sql, reader)
  }
}
