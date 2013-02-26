package com.socrata.datacoordinator.common
package soql

import java.util.concurrent.ExecutorService
import java.sql.Connection
import javax.sql.DataSource
import java.io.Reader

import scalaz._
import Scalaz._

import org.joda.time.DateTime

import com.socrata.soql.brita.{IdentifierFilter, AsciiIdentifierFilter}
import com.socrata.soql.types.{SoQLFixedTimestamp, SoQLID, SoQLType}
import com.socrata.datacoordinator.truth._
import com.socrata.datacoordinator.truth.sql.PostgresDataContext
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.loader.RowPreparer
import com.socrata.datacoordinator.truth.sql.DatasetMapLimits
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.sql.DatasetMapLimits

trait SoQLDataContext extends DataSchemaContext with DataWritingContext with DataReadingContext {
  type CT = SoQLType
  type CV = Any

  val columnNames = SoQLDataContext.ColumnNames
  import columnNames._

  val typeContext = SoQLTypeContext
  val systemColumns = Map[String, CT](
    systemId -> SoQLID,
    createdAt -> SoQLFixedTimestamp,
    updatedAt -> SoQLFixedTimestamp
  )
  val systemIdColumnName: String = systemId

  def isSystemColumn(name: String) = name.startsWith(":")

  def newRowLogCodec() = SoQLRowLogCodec

   def physicalColumnBaseBase(logicalName: String, systemColumn: Boolean) =
    AsciiIdentifierFilter(List(if(systemColumn) "s" else "u", logicalName)).take(datasetMapLimits.maximumPhysicalColumnBaseLength).replaceAll("_$", "").toLowerCase

  def isLegalLogicalName(name: String) =
    IdentifierFilter(name) == name && name.length <= datasetMapLimits.maximumLogicalColumnNameLength

  def rowPreparer(transactionStart: DateTime, schema: ColumnIdMap[ColumnInfo]) =
    new RowPreparer[CV] {
      def findCol(name: String) =
        schema.values.find(_.logicalName == name).getOrElse(sys.error(s"No $name column?")).systemId
      val idColumn = findCol(systemId)
      val createdAtColumn = findCol(createdAt)
      val updatedAtColumn = findCol(updatedAt)

      def prepareForInsert(row: Row, sid: RowId): Row = {
        val tmp = new MutableRow(row)
        tmp(idColumn) = sid
        tmp(createdAtColumn) = transactionStart
        tmp(updatedAtColumn) = transactionStart
        tmp.freeze()
      }

      def prepareForUpdate(row: Row): Row = {
        val tmp = new MutableRow(row)
        tmp -= idColumn
        tmp -= createdAtColumn
        tmp(updatedAtColumn) = transactionStart
        tmp.freeze()
      }
    }

  lazy val addSystemColumns: datasetMutator.DatasetM[Unit] = systemColumns.map { case (name, typ) =>
    import datasetMutator._
    addColumn(name, typeContext.nameFromType(typ), physicalColumnBaseBase(name, systemColumn = true)).flatMap { col =>
      if(col.logicalName == systemId) makeSystemPrimaryKey(col)
      else col.pure[DatasetM]
    }
  }.toList.sequenceU.map(_ => ())
}

object SoQLDataContext {
  object ColumnNames {
    val systemId = ":id"
    val createdAt = ":created_at"
    val updatedAt = ":updated_at"
  }
}

trait PostgresSoQLDataContext extends PostgresDataContext with SoQLDataContext with ExecutionContext {
  def sqlRepForColumn(physicalColumnBase: String, typ: CT) =
    SoQLRep.sqlRepFactories(typ)(physicalColumnBase)

  def withRows[T](datasetId: String)(f: Iterator[Row] => T): Option[T] = {
    val conn = dataSource.getConnection()
    try {
      conn.setReadOnly(true)
      conn.setAutoCommit(false)
      val datasetMap = new com.socrata.datacoordinator.truth.metadata.sql.PostgresDatasetMapReader(conn)
      datasetMap.datasetInfo(datasetId).map { di =>
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

trait CsvSoQLDataContext extends CsvDataContext with SoQLDataContext {
  def csvRepForColumn(typ: CT) =
    SoQLRep.csvRepFactories(typ)
}

trait JsonSoQLDataContext extends JsonDataContext with SoQLDataContext { this: PostgresDataContext =>
  import com.rojoma.json._

  def jsonRepForColumn(name: String, typ: CT) =
    SoQLRep.jsonRepFactories(typ)(name)

  def jsonSchema(datasetId: String) = {
    val conn = dataSource.getConnection()
    try {
      conn.setReadOnly(true)
      conn.setAutoCommit(false)
      val datasetMap = new com.socrata.datacoordinator.truth.metadata.sql.PostgresDatasetMapReader(conn)
      datasetMap.datasetInfo(datasetId).map { di =>
        val copy = datasetMap.latest(di)
        val schema = datasetMap.schema(copy)
        schema.mapValuesStrict(jsonRepForColumn)
      }
    }
  }

  def toJObject(schema: ColumnIdMap[json.JsonColumnWriteRep[CT, CV]], row: Row): ast.JObject = {
    val m = new scala.collection.mutable.HashMap[String, ast.JValue]
    row.foreach { case (columnId, cv) =>
      val rep = schema(columnId)
      m(rep.name) = rep.toJValue(cv)
    }
    ast.JObject(m)
  }
}
