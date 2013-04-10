package com.socrata.datacoordinator.primary

import java.sql.Connection
import java.io.{Reader, File}

import com.socrata.csv.CSVIterator

import org.postgresql.ds._
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.common.soql._
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth._
import com.socrata.datacoordinator.{Row, MutableRow}
import com.socrata.datacoordinator.common.{StandardObfuscationKeyGenerator, StandardDatasetMapLimits}
import org.postgresql.PGConnection
import com.socrata.soql.brita.IdentifierFilter
import com.socrata.datacoordinator.util.{StackedTimingReport, LoggedTimingReport}
import org.slf4j.LoggerFactory
import scala.concurrent.duration.Duration
import com.socrata.soql.environment.{ColumnName, TypeName}
import com.socrata.datacoordinator.id.RowId

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

  val datasetName =  args.lift(1).getOrElse("crimes")
  val inputFile = args.lift(2).getOrElse("/home/robertm/chicagocrime.csv")

  val ds = new PGSimpleDataSource
  ds.setServerName("localhost")
  ds.setPortNumber(5432)
  ds.setUser("blist")
  ds.setPassword("blist")
  ds.setDatabaseName("robertm")

  val executor = java.util.concurrent.Executors.newCachedThreadPool()
  try {

    val dataContextRaw = new PostgresSoQLDataContext with CsvSoQLDataContext {
      val obfuscationKeyGenerator = StandardObfuscationKeyGenerator
      val initialRowId = new RowId(0L)
      val dataSource = ds
      val executorService = executor
      def copyIn(conn: Connection, sql: String, input: Reader): Long =
        conn.asInstanceOf[PGConnection].getCopyAPI.copyIn(sql, input)
      def tablespace(s: String) = None
      val datasetMapLimits = StandardDatasetMapLimits
      val datasetMutatorLockTimeout = Duration.Inf
      val timingReport = new LoggedTimingReport(LoggerFactory.getLogger("timing-report")) with StackedTimingReport
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

    try { datasetCreator.createDataset(datasetName, user, "en_US") }
    catch { case _: DatasetAlreadyExistsException => /* pass */ }
    using(CSVIterator.fromFile(new File(inputFile))) { it =>
      val NumberT = dataContext.typeContext.typeNamespace.typeForUserType(TypeName("number")).get
      val TextT = dataContext.typeContext.typeNamespace.typeForUserType(TypeName("text")).get
      val BooleanT = dataContext.typeContext.typeNamespace.typeForUserType(TypeName("boolean")).get
      val FloatingTimestampT = dataContext.typeContext.typeNamespace.typeForUserType(TypeName("floating_timestamp")).get
      val LocationT = dataContext.typeContext.typeNamespace.typeForUserType(TypeName("location")).get
      val types = Map(
        ColumnName("id") -> NumberT,
        ColumnName("case_number") -> TextT,
        ColumnName("date") -> FloatingTimestampT,
        ColumnName("block") -> TextT,
        ColumnName("iucr") -> TextT,
        ColumnName("primary_type") -> TextT,
        ColumnName("description") -> TextT,
        ColumnName("location_description") -> TextT,
        ColumnName("arrest") -> BooleanT,
        ColumnName("domestic") -> BooleanT,
        ColumnName("beat") -> TextT,
        ColumnName("district") -> TextT,
        ColumnName("ward") -> NumberT,
        ColumnName("community_area") -> TextT,
        ColumnName("fbi_code") -> TextT,
        ColumnName("x_coordinate") -> NumberT,
        ColumnName("y_coordinate") -> NumberT,
        ColumnName("year") -> NumberT,
        ColumnName("updated_on") -> FloatingTimestampT,
        ColumnName("latitude") -> NumberT,
        ColumnName("longitude") -> NumberT,
        ColumnName("location") -> LocationT
      )
      val headers = it.next().map { t => ColumnName(IdentifierFilter(t)) }
      val schema = columnAdder.addToSchema(datasetName, headers.map { x => x -> types(x) }.toMap, user)
      primaryKeySetter.makePrimaryKey(datasetName, ColumnName("id"), user)
      val start = System.nanoTime()
      upserter.upsert(datasetName, user) { _ =>
        val plan = rowDecodePlan(dataContext)(schema, headers)
        it.take(10).map { row =>
          val result = plan(row)
          if(result._1.nonEmpty) throw new Exception("Error decoding row; unable to decode columns: " + result._1.mkString(", "))
          result._2
        }.map(Right(_))
      }
      val end = System.nanoTime()
      println(s"Upsert took ${(end - start) / 1000000L}ms")
      publisher.publish(datasetName, None, user)
      workingCopyCreator.copyDataset(datasetName, user, copyData = true)
      val ci = for {
        dataContext.datasetMutator.CopyOperationComplete(ctx) <- dataContext.datasetMutator.dropCopy(user)(datasetName)
      } yield {
        ctx.copyInfo.unanchored
      }
      workingCopyCreator.copyDataset(datasetName, user, copyData = true)
      println(ci)
    }
  } finally {
    executor.shutdown()
  }

  def rowDecodePlan(ctx: CsvDataContext)(schema: Map[ColumnName, ColumnInfo[ctx.CT]], headers: IndexedSeq[ColumnName]): IndexedSeq[String] => (Seq[ColumnName], Row[ctx.CV]) = {
    val colInfo = headers.zipWithIndex.map { case (header, idx) =>
      val ci = schema(header)
      (header, ci.systemId, ctx.csvRepForColumn(ci), Array(idx) : IndexedSeq[Int])
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
