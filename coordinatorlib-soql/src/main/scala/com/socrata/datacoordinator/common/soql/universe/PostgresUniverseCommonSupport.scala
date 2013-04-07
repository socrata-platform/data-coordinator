package com.socrata.datacoordinator
package common.soql.universe

import com.socrata.datacoordinator.truth.universe.sql.CommonSupport
import java.sql.Connection
import java.io.Reader
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import java.util.concurrent.ExecutorService
import com.socrata.soql.types.{SoQLFixedTimestamp, SoQLID, SoQLValue, SoQLType}
import com.socrata.datacoordinator.common.soql._
import com.socrata.soql.environment.{ColumnName, TypeName}
import org.joda.time.DateTime
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.loader.RowPreparer
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.truth.metadata.ColumnInfo

class PostgresUniverseCommonSupport(val executor: ExecutorService, val tablespace: String => Option[String], val copyInProvider: (Connection, String, Reader) => Long, val obfuscationKeyGenerator: () => Array[Byte], val initialRowId: RowId) extends CommonSupport[SoQLType, SoQLValue] {
  val typeContext = SoQLTypeContext

  def repFor(ci: ColumnInfo) =
    SoQLRep.sqlRepFactories(SoQLType.typesByName(ci.typeName))(ci.physicalColumnBase)

  def newRowCodec() = SoQLRowLogCodec

  def isSystemColumn(ci: ColumnInfo): Boolean = ci.logicalName.name.startsWith(":")

  val systemId = ColumnName(":id")
  val createdAt = ColumnName(":created_at")
  val updatedAt = ColumnName(":updated_at")

  def rowPreparer(transactionStart: DateTime, schema: ColumnIdMap[ColumnInfo]): RowPreparer[SoQLValue] =
    new RowPreparer[SoQLValue] {
      def findCol(name: ColumnName) =
        schema.values.find(_.logicalName == name).getOrElse(sys.error(s"No $name column?")).systemId

      val idColumn = findCol(systemId)
      val createdAtColumn = findCol(createdAt)
      val updatedAtColumn = findCol(updatedAt)

      def prepareForInsert(row: Row[SoQLValue], sid: RowId) = {
        val tmp = new MutableRow(row)
        tmp(idColumn) = SoQLID(sid.underlying)
        tmp(createdAtColumn) = SoQLFixedTimestamp(transactionStart)
        tmp(updatedAtColumn) = SoQLFixedTimestamp(transactionStart)
        tmp.freeze()
      }

      def prepareForUpdate(row: Row[SoQLValue]) = {
        val tmp = new MutableRow[SoQLValue](row)
        tmp -= idColumn
        tmp -= createdAtColumn
        tmp(updatedAtColumn) = SoQLFixedTimestamp(transactionStart)
        tmp.freeze()
      }
    }
}
