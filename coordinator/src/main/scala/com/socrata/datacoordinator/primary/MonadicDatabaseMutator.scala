package com.socrata.datacoordinator
package primary

import org.joda.time.DateTime

import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.truth.MonadicDatasetMutator
import com.socrata.datacoordinator.truth.sql.PostgresDatabaseMutator
import java.sql.Connection
import com.socrata.id.numeric.IdProvider
import com.socrata.datacoordinator.truth.loader.sql.{PostgresSqlLoaderProvider, AbstractSqlLoaderProvider}
import org.postgresql.PGConnection
import java.io.Reader
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresGlobalLog, PostgresDatasetMapWriter}

object Test extends App {
  import org.postgresql.ds._
  import com.socrata.datacoordinator.truth.sql.SqlColumnRep
  import com.socrata.datacoordinator.common.soql._
  import com.socrata.datacoordinator.common.StandardDatasetMapLimits
  import com.socrata.soql.types.SoQLType
  import com.socrata.datacoordinator.id.RowId
  import com.socrata.datacoordinator.util.CloseableIterator
  import com.rojoma.simplearm.util._

  val ds = new PGSimpleDataSource
  ds.setServerName("localhost")
  ds.setPortNumber(5432)
  ds.setUser("blist")
  ds.setPassword("blist")
  ds.setDatabaseName("robertm")

  val typeContext = SoQLTypeContext
  val soqlRepFactory = SoQLRep.sqlRepFactories.keys.foldLeft(Map.empty[SoQLType, String => SqlColumnRep[SoQLType, Any]]) { (acc, typ) =>
    acc + (typ -> SoQLRep.sqlRepFactories(typ))
  }
  def genericRepFor(columnInfo: ColumnInfo): SqlColumnRep[SoQLType, Any] =
    soqlRepFactory(typeContext.typeFromName(columnInfo.typeName))(columnInfo.physicalColumnBase)

  def rowPreparer(now: DateTime, schema: ColumnIdMap[ColumnInfo]): RowPreparer[Any] =
    new RowPreparer[Any] {
      def findCol(name: String) =
        schema.values.iterator.find(_.logicalName == name).getOrElse(sys.error(s"No $name column?")).systemId
      val idColumn = findCol(SystemColumns.id)
      val createdAtColumn = findCol(SystemColumns.createdAt)
      val updatedAtColumn = findCol(SystemColumns.updatedAt)

      def prepareForInsert(row: Row[Any], sid: RowId): Row[Any] = {
        val tmp = new MutableRow[Any](row)
        tmp(idColumn) = sid
        tmp(createdAtColumn) = now
        tmp(updatedAtColumn) = now
        tmp.freeze()
      }

      def prepareForUpdate(row: Row[Any]): Row[Any] = {
        val tmp = new MutableRow[Any](row)
        tmp(updatedAtColumn) = now
        tmp.freeze()
      }
    }

  val executor = java.util.concurrent.Executors.newCachedThreadPool()

  val loaderProvider = new AbstractSqlLoaderProvider(executor, typeContext, genericRepFor, _.logicalName.startsWith(":")) with PostgresSqlLoaderProvider[SoQLType, Any] {
    def copyIn(conn: Connection, sql: String, reader: Reader): Long =
      conn.asInstanceOf[PGConnection].getCopyAPI.copyIn(sql, reader)
  }

  def loaderFactory(conn: Connection, now: DateTime, copy: CopyInfo, schema: ColumnIdMap[ColumnInfo], idProvider: IdProvider, logger: Logger[Any]): Loader[Any] = {
    loaderProvider(conn, copy, schema, rowPreparer(now, schema), idProvider, logger)
  }

  val ll = new PostgresDatabaseMutator(ds, genericRepFor, () => SoQLRowLogCodec, new PostgresDatasetMapWriter(_), new PostgresGlobalLog(_), loaderFactory, Function.const(None))
  val highlevel = MonadicDatasetMutator(ll)

  com.rojoma.simplearm.util.using(ds.getConnection()) { conn =>
    com.socrata.datacoordinator.truth.sql.DatabasePopulator.populate(conn, StandardDatasetMapLimits)
  }

  import highlevel._
  val name = System.currentTimeMillis().toString
  val (col1Id, col2Id) = for(ctx <- createDataset(as = "robertm")(name, "m")) yield {
    import ctx._
    val id = makeSystemPrimaryKey(addColumn(":id", "row_identifier", "id"))
    val created_at = addColumn(":created_at", "fixed_timestamp", "created")
    val updated_at = addColumn(":updated_at", "fixed_timestamp", "updated")
    val col1 = makeUserPrimaryKey(addColumn("col1", "number", "first"))
    val col2 = addColumn("col2", "text", "second")
    val report = upsert(Iterator(
      Right(Row(col1.systemId -> BigDecimal(5), col2.systemId -> "hello")),
      Right(Row(col1.systemId -> BigDecimal(6), col2.systemId -> "hello"))
    ))
    publish()
    (col1.systemId, col2.systemId)
  }

  val report2 = for {
    ctxOpt <- createCopy(as = "robertm")(name, copyData = false)
    ctx <- ctxOpt
  } yield {
    import ctx._
    val col1 = schema(col1Id)
    val col2 = schema(col2Id)
    val report = upsert(Iterator(
      Right(Row(col1.systemId -> BigDecimal(6), col2.systemId -> "goodbye")),
      Right(Row(col1.systemId -> BigDecimal(7), col2.systemId -> "world"))
    ))
    publish()
    report
  }
  println(report2)

  executor.shutdown()
}
