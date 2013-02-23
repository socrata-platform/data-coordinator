package com.socrata.datacoordinator
package primary

import scalaz.effect.IO
import org.joda.time.DateTime

import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.truth.MonadicDatasetMutator
import com.socrata.datacoordinator.truth.sql.PostgresMonadicDatabaseMutator
import java.sql.Connection
import com.socrata.id.numeric.IdProvider
import com.socrata.datacoordinator.truth.loader.sql.{PostgresSqlLoaderProvider, AbstractSqlLoaderProvider}
import org.postgresql.core.BaseConnection
import java.io.Reader

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
  val soqlRepFactory = SoQLRep.repFactories.keys.foldLeft(Map.empty[SoQLType, String => SqlColumnRep[SoQLType, Any]]) { (acc, typ) =>
    acc + (typ -> SoQLRep.repFactories(typ))
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
      conn.asInstanceOf[BaseConnection].getCopyAPI.copyIn(sql, reader)
  }

  def loaderFactory(conn: Connection, now: DateTime, copy: CopyInfo, schema: ColumnIdMap[ColumnInfo], idProvider: IdProvider, logger: Logger[Any]): Loader[Any] = {
    loaderProvider(conn, copy, schema, rowPreparer(now, schema), idProvider, logger)
  }

  val ll = new PostgresMonadicDatabaseMutator(ds, genericRepFor, () => SoQLRowLogCodec, loaderFactory, Function.const(None))
  val highlevel = MonadicDatasetMutator(ll)

  com.rojoma.simplearm.util.using(ds.getConnection()) { conn =>
    com.socrata.datacoordinator.truth.sql.DatabasePopulator.populate(conn, StandardDatasetMapLimits)
  }

  import highlevel._
  val name = System.currentTimeMillis().toString
  val (col1Id, col2Id) = creatingDataset(as = "robertm")(name, "m") {
    for {
      id <- addColumn(":id", "row_identifier", "id").flatMap(makeSystemPrimaryKey)
      created_at <- addColumn(":created_at", "fixed_timestamp", "created")
      updated_at <- addColumn(":updated_at", "fixed_timestamp", "updated")
      col1 <- addColumn("col1", "number", "first").flatMap(makeUserPrimaryKey)
      col2 <- addColumn("col2", "text", "second")
      report <- upsert(IO(Iterator(
        Right(Row(col1.systemId -> BigDecimal(5), col2.systemId -> "hello")),
        Right(Row(col1.systemId -> BigDecimal(6), col2.systemId -> "hello"))
      )))
      _ <- publish
    } yield (col1.systemId, col2.systemId)
  }.unsafePerformIO()

  val report2 = creatingCopy(as = "robertm")(name, copyData = false) {
    for {
      col1 <- schema.map(_(col1Id))
      col2 <- schema.map(_(col2Id))
      report <- upsert(IO(Iterator(
        Right(Row(col1.systemId -> BigDecimal(6), col2.systemId -> "goodbye")),
        Right(Row(col1.systemId -> BigDecimal(7), col2.systemId -> "world"))
      )))
      _ <- publish
    } yield report
  }.unsafePerformIO()
  println(report2)

  executor.shutdown()
}
