package com.socrata.datacoordinator.primary

import java.sql.{Connection, DriverManager}
import java.util.concurrent.Executors
import java.io.{Reader, File, Closeable}

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import com.rojoma.simplearm.{SimpleArm, Managed}
import com.rojoma.simplearm.util._

import com.socrata.soql.types._
import com.socrata.id.numeric.IdProvider
import com.socrata.csv.CSVIterator

import org.postgresql.ds._
import com.socrata.soql.types.SoQLType
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.common.soql._
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.truth._
import com.socrata.datacoordinator.truth.sql.{DatasetMapLimits, PostgresMonadicDatabaseMutator, SqlColumnRep}
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.{Row, MutableRow}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.loader.sql.{PostgresSqlLoaderProvider, AbstractSqlLoaderProvider}
import com.socrata.datacoordinator.common.StandardDatasetMapLimits
import scalaz.effect.IO
import org.postgresql.PGConnection
import com.socrata.soql.brita.IdentifierFilter

object ChicagoCrimesLoadScript extends App {
  val url =
  // "jdbc:postgresql://10.0.5.104:5432/robertm"
    "jdbc:postgresql://localhost:5432/robertm"
  val username =
  // "robertm"
    "blist"
  val pwd =
  // "lof9afw3"
    "blist"

  val ds = new PGSimpleDataSource
  ds.setServerName("localhost")
  ds.setPortNumber(5432)
  ds.setUser("blist")
  ds.setPassword("blist")
  ds.setDatabaseName("robertm")

  val executor = java.util.concurrent.Executors.newCachedThreadPool()
  try {

    val dataContextRaw = new PostgresSoQLDataContext with CsvSoQLDataContext {
      val dataSource = ds
      val executorService = executor
      def copyIn(conn: Connection, sql: String, input: Reader): Long =
        conn.asInstanceOf[PGConnection].getCopyAPI.copyIn(sql, input)
      def tablespace(s: String) = None
      val datasetMapLimits = StandardDatasetMapLimits
    }

    com.rojoma.simplearm.util.using(ds.getConnection()) { conn =>
      com.socrata.datacoordinator.truth.sql.DatabasePopulator.populate(conn, StandardDatasetMapLimits)
    }

    val dataContext: DataWritingContext with CsvDataContext = dataContextRaw

    val datasetCreator = new DatasetCreator(dataContext)

    val columnAdder = ColumnAdder[dataContext.CT](dataContext)

    val primaryKeySetter = new PrimaryKeySetter(dataContext.datasetMutator)

    val upserter = new Upserter(dataContext.datasetMutator)

    val publisher = new Publisher(dataContext.datasetMutator)

    val workingCopyCreator = new WorkingCopyCreator(dataContext.datasetMutator)

    // Above this can be re-used for every query

    val user = "robertm"

    try { datasetCreator.createDataset("crimes", user).unsafePerformIO() }
    catch { case _: DatasetAlreadyExistsException => /* pass */ }
    using(CSVIterator.fromFile(new File("/home/robertm/chicagocrime.csv"))) { it =>
      val NumberT = dataContext.typeContext.typeFromName("number")
      val TextT = dataContext.typeContext.typeFromName("text")
      val BooleanT = dataContext.typeContext.typeFromName("boolean")
      val FixedTimestampT = dataContext.typeContext.typeFromName("fixed_timestamp")
      val LocationT = dataContext.typeContext.typeFromName("location")
      val types = Map(
        "id" -> NumberT,
        "case_number" -> TextT,
        "date" -> FixedTimestampT,
        "block" -> TextT,
        "iucr" -> TextT,
        "primary_type" -> TextT,
        "description" -> TextT,
        "location_description" -> TextT,
        "arrest" -> BooleanT,
        "domestic" -> BooleanT,
        "beat" -> TextT,
        "district" -> TextT,
        "ward" -> TextT,
        "community_area" -> TextT,
        "fbi_code" -> TextT,
        "x_coordinate" -> NumberT,
        "y_coordinate" -> NumberT,
        "year" -> TextT,
        "updated_on" -> FixedTimestampT,
        "latitude" -> NumberT,
        "longitude" -> NumberT,
        "location" -> LocationT
      )
      val headers = it.next().map(IdentifierFilter(_).toLowerCase)
      val schema = columnAdder.addToSchema("crimes", headers.map { x => x -> types(x) }.toMap, user).unsafePerformIO().mapValues { ci =>
        (ci, dataContext.typeContext.typeFromName(ci.typeName))
      }.toMap
      primaryKeySetter.makePrimaryKey("crimes", "id", user).unsafePerformIO()
      val start = System.nanoTime()
      upserter.upsert("crimes", user) { _ =>
        val plan = rowDecodePlan(dataContext)(schema, headers)
        IO(it.map { row =>
          val result = plan(row)
          if(result._1.nonEmpty) throw new Exception("Error decoding row; unable to decode columns: " + result._1.mkString(", "))
          result._2
        }.map(Right(_)))
      }.unsafePerformIO()
      val end = System.nanoTime()
      println(s"Upsert took ${(end - start) / 1000000L}ms")
      publisher.publish("crimes", user).unsafePerformIO()
      workingCopyCreator.copyDataset("crimes", user, copyData = true).unsafePerformIO()
      val ci = dataContext.datasetMutator.withDataset(user)("crimes") {
        dataContext.datasetMutator.drop.map(_ => dataContext.datasetMutator.copyInfo)
      }.unsafePerformIO()
      workingCopyCreator.copyDataset("crimes", user, copyData = true).unsafePerformIO()
      println(ci)
    }
  } finally {
    executor.shutdown()
  }

  def rowDecodePlan(ctx: CsvDataContext)(schema: Map[String, (ColumnInfo, ctx.CT)], headers: IndexedSeq[String]): IndexedSeq[String] => (Seq[String], Row[ctx.CV]) = {
    val colInfo = headers.zipWithIndex.map { case (header, idx) =>
      val (ci, typ) = schema(header)
      (header, ci.systemId, ctx.csvRepForColumn(typ), Array(idx) : IndexedSeq[Int])
    }
    (row: IndexedSeq[String]) => {
      val result = new MutableRow[ctx.CV]
      val bads = colInfo.flatMap { case (header, systemId, rep, indices) =>
        try {
          result += systemId -> rep.decode(row, indices).get
          Nil
        } catch { case e: Exception => List(header) }
      }
      (bads, result.freeze())
    }
  }
}
