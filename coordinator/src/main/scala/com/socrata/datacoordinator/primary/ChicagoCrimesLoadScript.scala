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
import org.postgresql.core.BaseConnection

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

  def convertNum(x: String) =
    if(x.isEmpty) SoQLNullValue
    else BigDecimal(x)

  def convertBool(x: String) =
    if(x.isEmpty) SoQLNullValue
    else java.lang.Boolean.parseBoolean(x)

  val tsParser = DateTimeFormat.forPattern("MM/dd/yyyy hh:mm aa").withZoneUTC

  def convertTS(x: String) =
    if(x.isEmpty) SoQLNullValue
    else tsParser.parseDateTime(x)

  val fmt = """^\(([0-9.-]+), ([0-9.-]+)\)$""".r
  def convertLoc(x: String) =
    if(x.isEmpty) SoQLNullValue
    else {
      val mtch = fmt.findFirstMatchIn(x).get
      SoQLLocationValue(mtch.group(1).toDouble, mtch.group(2).toDouble)
    }

  val converter: Map[SoQLType, String => Any] = Map (
    SoQLText -> identity[String],
    SoQLNumber -> convertNum,
    SoQLBoolean -> convertBool,
    SoQLFixedTimestamp -> convertTS,
    SoQLLocation -> convertLoc
  )

  val executor = java.util.concurrent.Executors.newCachedThreadPool()
  try {

    val dataContext: DataWritingContext[SoQLType, Any] = new PostgresSoQLDataContext {
      val dataSource = ds
      val executorService = executor
      def copyIn(conn: Connection, sql: String, input: Reader): Long =
        conn.asInstanceOf[BaseConnection].getCopyAPI.copyIn(sql, input)
      def tablespace(s: String) = None
      val datasetMapLimits = StandardDatasetMapLimits
    }

    com.rojoma.simplearm.util.using(ds.getConnection()) { conn =>
      com.socrata.datacoordinator.truth.sql.DatabasePopulator.populate(conn, StandardDatasetMapLimits)
    }

    val datasetCreator = new DatasetCreator(dataContext)

    val columnAdder = new ColumnAdder(dataContext)

    val primaryKeySetter = new PrimaryKeySetter(dataContext.datasetMutator)

    val upserter = new Upserter(dataContext.datasetMutator)

    val publisher = new Publisher(dataContext.datasetMutator)

    val workingCopyCreator = new WorkingCopyCreator(dataContext.datasetMutator)

    // Above this can be re-used for every query

    val user = "robertm"

    try { datasetCreator.createDataset("crimes", user).unsafePerformIO() }
    catch { case _: DatasetAlreadyExistsException => /* pass */ }
    using(CSVIterator.fromFile(new File("/home/robertm/chicagocrime.csv"))) { it =>
      val types = Map(
        "ID" -> SoQLNumber,
        "Case Number" -> SoQLText,
        "Date" -> SoQLFixedTimestamp,
        "Block" -> SoQLText,
        "IUCR" -> SoQLText,
        "Primary Type" -> SoQLText,
        "Description" -> SoQLText,
        "Location Description" -> SoQLText,
        "Arrest" -> SoQLBoolean,
        "Domestic" -> SoQLBoolean,
        "Beat" -> SoQLText,
        "District" -> SoQLText,
        "Ward" -> SoQLText,
        "Community Area" -> SoQLText,
        "FBI Code" -> SoQLText,
        "X Coordinate" -> SoQLNumber,
        "Y Coordinate" -> SoQLNumber,
        "Year" -> SoQLText,
        "Updated On" -> SoQLFixedTimestamp,
        "Latitude" -> SoQLNumber,
        "Longitude" -> SoQLNumber,
        "Location" -> SoQLLocation
      )
      val headers = it.next()
      val schema = columnAdder.addToSchema("crimes", headers.map { x => x -> types(x) }.toMap, user).unsafePerformIO().mapValues { ci =>
        (ci, dataContext.typeContext.typeFromName(ci.typeName))
      }.toMap
      primaryKeySetter.makePrimaryKey("crimes", "ID", user).unsafePerformIO()
      val start = System.nanoTime()
      upserter.upsert("crimes", user) { _ =>
        IO(it.take(10).map(transformToRow(schema, headers, _)).map(Right(_)))
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

  def noopManagement[T](t: T): SimpleArm[T] =
    new SimpleArm[T] {
      def flatMap[B](f: (T) => B): B = f(t)
    }

  def transformToRow(schema: Map[String, (ColumnInfo, SoQLType)], headers: IndexedSeq[String], row: IndexedSeq[String]): Row[Any] = {
    assert(headers.length == row.length, "Bad row; different number of columns from the headers")
    val result = new MutableRow[Any]
    (headers, row).zipped.foreach { (header, value) =>
      val (ci,typ) = schema(header)
      result += ci.systemId -> (try { converter(typ)(value) }
                                catch { case e: Exception => throw new Exception("Problem converting " + header + ": " + value, e) })
    }
    result.freeze()
  }
}
