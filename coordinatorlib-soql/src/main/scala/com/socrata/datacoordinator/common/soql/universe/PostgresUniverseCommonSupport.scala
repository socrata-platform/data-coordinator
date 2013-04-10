package com.socrata.datacoordinator
package common.soql.universe

import java.util.concurrent.ExecutorService
import java.sql.Connection
import java.io.Reader

import org.joda.time.DateTime

import com.socrata.datacoordinator.truth.universe.sql.CommonSupport
import com.socrata.datacoordinator.truth.metadata.{AbstractColumnInfoLike, ColumnInfo}
import com.socrata.soql.types.{SoQLFixedTimestamp, SoQLID, SoQLValue, SoQLType}
import com.socrata.datacoordinator.common.soql._
import com.socrata.soql.environment.ColumnName
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.loader.RowPreparer
import com.socrata.datacoordinator.id.RowId

class PostgresUniverseCommonSupport(val executor: ExecutorService, val tablespace: String => Option[String], val copyInProvider: (Connection, String, Reader) => Long, val obfuscationKeyGenerator: () => Array[Byte], val initialRowId: RowId) extends CommonSupport[SoQLType, SoQLValue] {
  val typeContext = SoQLTypeContext

  def repFor(ci: ColumnInfo[SoQLType]) =
    SoQLRep.sqlRep(ci)

  def newRowCodec() = SoQLRowLogCodec

  def isSystemColumn(ci: AbstractColumnInfoLike): Boolean = ci.logicalName.name.startsWith(":")

  val systemId = ColumnName(":id")
  val createdAt = ColumnName(":created_at")
  val updatedAt = ColumnName(":updated_at")

  def rowPreparer(transactionStart: DateTime, schema: ColumnIdMap[AbstractColumnInfoLike]): RowPreparer[SoQLValue] =
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
