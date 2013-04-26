package com.socrata.datacoordinator.primary

import java.io.File

import com.socrata.thirdparty.opencsv.CSVIterator

import org.postgresql.ds._
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.{Row, MutableRow}
import com.socrata.datacoordinator.common.{SoQLCommon, StandardDatasetMapLimits}
import com.socrata.soql.brita.IdentifierFilter
import com.socrata.datacoordinator.util.{StackedTimingReport, LoggedTimingReport}
import org.slf4j.LoggerFactory
import com.socrata.soql.environment.{ColumnName, TypeName}
import com.socrata.datacoordinator.truth.universe.sql.PostgresCopyIn
import com.socrata.datacoordinator.truth.csv.CsvColumnReadRep
import com.socrata.datacoordinator.common.soql.SoQLRep

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

    val common = new SoQLCommon(
      ds,
      PostgresCopyIn,
      executor,
      Function.const(None),
      new LoggedTimingReport(LoggerFactory.getLogger("timing-report")) with StackedTimingReport
    )

    com.rojoma.simplearm.util.using(ds.getConnection()) { conn =>
      com.socrata.datacoordinator.truth.sql.DatabasePopulator.populate(conn, StandardDatasetMapLimits)
    }

    val datasetCreator = new DatasetCreator(common.universe, common.systemSchema, common.SystemColumnNames.id, common.physicalColumnBaseBase)

    val columnAdder = new ColumnAdder(common.universe, common.physicalColumnBaseBase)

    val primaryKeySetter = new PrimaryKeySetter(common.universe)

    val upserter = new Upserter(common.universe)

    val publisher = new Publisher(common.universe)

    val workingCopyCreator = new WorkingCopyCreator(common.universe)

    // Above this can be re-used for every query

    val user = "robertm"

    try { datasetCreator.createDataset(datasetName, user, "en_US") }
    catch { case _: DatasetAlreadyExistsException => /* pass */ }
    using(CSVIterator.fromFile(new File(inputFile))) { it =>
      val NumberT = common.typeContext.typeNamespace.typeForUserType(TypeName("number")).get
      val TextT = common.typeContext.typeNamespace.typeForUserType(TypeName("text")).get
      val BooleanT = common.typeContext.typeNamespace.typeForUserType(TypeName("boolean")).get
      val FloatingTimestampT = common.typeContext.typeNamespace.typeForUserType(TypeName("floating_timestamp")).get
      val LocationT = common.typeContext.typeNamespace.typeForUserType(TypeName("location")).get
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
        val plan = rowDecodePlan(schema, headers, SoQLRep.csvRep)
        it.map { row =>
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
        u <- common.universe
        u.datasetMutator.CopyOperationComplete(ctx) <- u.datasetMutator.dropCopy(user)(datasetName, _ => ())
      } yield {
        ctx.copyInfo.unanchored
      }
      workingCopyCreator.copyDataset(datasetName, user, copyData = true)
      println(ci)
    }
  } finally {
    executor.shutdown()
  }

  def rowDecodePlan[CT, CV](schema: Map[ColumnName, ColumnInfo[CT]], headers: IndexedSeq[ColumnName], csvRepForColumn: ColumnInfo[CT] => CsvColumnReadRep[CT, CV]): IndexedSeq[String] => (Seq[ColumnName], Row[CV]) = {
    val colInfo = headers.zipWithIndex.map { case (header, idx) =>
      val ci = schema(header)
      (header, ci.systemId, csvRepForColumn(ci), Array(idx) : IndexedSeq[Int])
    }
    (row: IndexedSeq[String]) => {
      val result = new MutableRow[CV]
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
