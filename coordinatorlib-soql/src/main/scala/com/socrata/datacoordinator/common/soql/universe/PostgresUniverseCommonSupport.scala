package com.socrata.datacoordinator
package common.soql.universe

import com.socrata.datacoordinator.truth.universe.sql.CommonSupport
import java.sql.Connection
import java.io.Reader
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import java.util.concurrent.ExecutorService
import com.socrata.soql.types.SoQLType
import com.socrata.datacoordinator.common.soql.{SoQLRowLogCodec, SoQLRep, SoQLTypeContext}
import com.socrata.soql.environment.{ColumnName, TypeName}
import org.joda.time.DateTime
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.loader.RowPreparer
import com.socrata.datacoordinator.id.{RowIdProcessor, RowId}

class PostgresUniverseCommonSupport(val executor: ExecutorService, val rowIdProcessor: RowIdProcessor, val tablespace: String => Option[String], val copyInProvider: (Connection, String, Reader) => Long, soqlReps: SoQLRep) extends CommonSupport[SoQLType, Any] {
  val typeContext = SoQLTypeContext

  def repFor(ci: ColumnInfo) =
    soqlReps.sqlRepFactories(SoQLType.typesByName(ci.typeName))(ci.physicalColumnBase)

  def newRowCodec() = SoQLRowLogCodec

  def isSystemColumn(ci: ColumnInfo): Boolean = ci.logicalName.name.startsWith(":")

  val systemId = ColumnName(":id")
  val createdAt = ColumnName(":created_at")
  val updatedAt = ColumnName(":updated_at")

  def rowPreparer(transactionStart: DateTime, schema: ColumnIdMap[ColumnInfo]): RowPreparer[Any] =
    new RowPreparer[Any] {
      def findCol(name: ColumnName) =
        schema.values.find(_.logicalName == name).getOrElse(sys.error(s"No $name column?")).systemId

      val idColumn = findCol(systemId)
      val createdAtColumn = findCol(createdAt)
      val updatedAtColumn = findCol(updatedAt)

      def prepareForInsert(row: Row[Any], sid: RowId) = {
        val tmp = new MutableRow(row)
        tmp(idColumn) = sid
        tmp(createdAtColumn) = transactionStart
        tmp(updatedAtColumn) = transactionStart
        tmp.freeze()
      }

      def prepareForUpdate(row: Row[Any]) = {
        val tmp = new MutableRow[Any](row)
        tmp -= idColumn
        tmp -= createdAtColumn
        tmp(updatedAtColumn) = transactionStart
        tmp.freeze()
      }
    }
}
