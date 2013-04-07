package com.socrata.datacoordinator.common
package soql

import java.util.concurrent.ExecutorService
import java.sql.Connection
import javax.sql.DataSource
import java.io.Reader

import org.joda.time.DateTime

import com.socrata.soql.brita.{IdentifierFilter, AsciiIdentifierFilter}
import com.socrata.soql.types.{SoQLValue, SoQLFixedTimestamp, SoQLID, SoQLType}
import com.socrata.datacoordinator.truth._
import com.socrata.datacoordinator.truth.sql.{PostgresDataContext, DatasetMapLimits}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.loader.RowPreparer
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.soql.environment.{ColumnName, TypeName}
import com.socrata.datacoordinator.truth.json.JsonColumnRep

@deprecated("deprected", "now")
trait SoQLDataContext extends DataSchemaContext with DataWritingContext with DataReadingContext {
  type CT = SoQLType
  type CV = SoQLValue

  val columnNames = SoQLDataContext.ColumnNames
  import columnNames._

  val typeContext = SoQLTypeContext
  val systemColumns = Map[ColumnName, CT](
    systemId -> SoQLID,
    createdAt -> SoQLFixedTimestamp,
    updatedAt -> SoQLFixedTimestamp
  )
  val systemIdColumnName: ColumnName = systemId

  def isSystemColumn(name: ColumnName) = name.name.startsWith(":")

  def newRowLogCodec() = SoQLRowLogCodec

   def physicalColumnBaseBase(logicalName: ColumnName, systemColumn: Boolean) =
    AsciiIdentifierFilter(List(if(systemColumn) "s" else "u", logicalName.name)).take(datasetMapLimits.maximumPhysicalColumnBaseLength).replaceAll("_$", "").toLowerCase

  def isLegalLogicalName(name: ColumnName) =
    IdentifierFilter(name.name) == name.name && name.name.length <= datasetMapLimits.maximumLogicalColumnNameLength

  def rowPreparer(transactionStart: DateTime, schema: ColumnIdMap[ColumnInfo]) =
    new RowPreparer[CV] {
      def findCol(name: ColumnName) =
        schema.values.find(_.logicalName == name).getOrElse(sys.error(s"No $name column?")).systemId
      val idColumn = findCol(systemId)
      val createdAtColumn = findCol(createdAt)
      val updatedAtColumn = findCol(updatedAt)

      def prepareForInsert(row: Row, sid: RowId): Row = {
        val tmp = new MutableRow(row)
        tmp(idColumn) = SoQLID(sid.underlying)
        tmp(createdAtColumn) = SoQLFixedTimestamp(transactionStart)
        tmp(updatedAtColumn) = SoQLFixedTimestamp(transactionStart)
        tmp.freeze()
      }

      def prepareForUpdate(row: Row): Row = {
        val tmp = new MutableRow(row)
        tmp -= idColumn
        tmp -= createdAtColumn
        tmp(updatedAtColumn) = SoQLFixedTimestamp(transactionStart)
        tmp.freeze()
      }
    }

  def addSystemColumns(ctx: datasetMutator.MutationContext) {
    for((name, typ) <- systemColumns) {
      import ctx._
      val col = addColumn(name, typ, physicalColumnBaseBase(name, systemColumn = true))
      if(col.logicalName == systemId) makeSystemPrimaryKey(col)
    }
  }
}

@deprecated("deprected", "now")
object SoQLDataContext {
  object ColumnNames {
    val systemId = ColumnName(":id")
    val createdAt = ColumnName(":created_at")
    val updatedAt = ColumnName(":updated_at")
  }
}

@deprecated("deprected", "now")
trait PostgresSoQLDataContext extends PostgresDataContext with SoQLDataContext with ExecutionContext {
  def sqlRepForColumn(physicalColumnBase: String, typ: CT) =
    SoQLRep.sqlRepFactories(typ)(physicalColumnBase)

  def withRows[T](datasetName: String)(f: Iterator[Row] => T): Option[T] = {
    val conn = dataSource.getConnection()
    try {
      conn.setReadOnly(true)
      conn.setAutoCommit(false)
      val datasetMap = new com.socrata.datacoordinator.truth.metadata.sql.PostgresDatasetMapReader(conn, timingReport)
      for {
        datasetId <- datasetMap.datasetId(datasetName)
        di <- datasetMap.datasetInfo(datasetId)
      } yield {
        val copy = datasetMap.latest(di)
        val schema = datasetMap.schema(copy)
        val reps = schema.mapValuesStrict(sqlRepForColumn)
        val stmt = conn.createStatement()
        try {
          stmt.setFetchSize(1000)
          val rs = stmt.executeQuery("SELECT " + reps.values.flatMap(_.physColumns).mkString(",") + " FROM " + copy.dataTableName)
          try {
            def loop(): Stream[Row] = {
              if(rs.next()) {
                var i = 1
                val result = new MutableRow
                reps.foreach { case (systemId, rep) =>
                  val v = rep.fromResultSet(rs, i)
                  i += rep.physColumns.length
                  result(systemId) = v
                }
                result.freeze() #:: loop()
              } else {
                Stream.empty
              }
            }
            f(loop().iterator)
          } finally {
            rs.close()
          }
        } finally {
          stmt.close()
        }
      }
    } finally {
      conn.close()
    }
  }
}

@deprecated("deprected", "now")
trait CsvSoQLDataContext extends CsvDataContext with SoQLDataContext {
  def csvRepForColumn(typ: CT) =
    SoQLRep.csvRepFactories(typ)
}
