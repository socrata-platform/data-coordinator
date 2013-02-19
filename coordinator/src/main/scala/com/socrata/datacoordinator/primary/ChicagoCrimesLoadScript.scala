package com.socrata.datacoordinator.primary

import java.sql.{Connection, DriverManager}
import java.util.concurrent.Executors
import java.io.{File, Closeable}

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
import com.socrata.datacoordinator.truth.sql.{PostgresMonadicDatabaseMutator, SqlColumnRep}
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.{Row, MutableRow}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.loader.sql.{PostgresSqlLoaderProvider, AbstractSqlLoaderProvider}
import com.socrata.datacoordinator.common.StandardDatasetMapLimits
import scalaz.effect.IO

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

    val loaderProvider = new AbstractSqlLoaderProvider(executor, typeContext, genericRepFor, _.logicalName.startsWith(":")) with PostgresSqlLoaderProvider[SoQLType, Any]

    def loaderFactory(conn: Connection, now: DateTime, copy: CopyInfo, schema: ColumnIdMap[ColumnInfo], idProvider: IdProvider, logger: Logger[Any]): Loader[Any] = {
      loaderProvider(conn, copy, schema, rowPreparer(now, schema), idProvider, logger)
    }

    val openConnection = IO(ds.getConnection())
    val ll = new PostgresMonadicDatabaseMutator(openConnection, genericRepFor, () => SoQLRowLogCodec, loaderFactory)
    val highlevel = MonadicDatasetMutator(ll)

    com.rojoma.simplearm.util.using(openConnection.unsafePerformIO()) { conn =>
      com.socrata.datacoordinator.truth.sql.DatabasePopulator.populate(conn, StandardDatasetMapLimits)
    }

    val datasetCreator = new DatasetCreator(highlevel, typeContext.nameFromType, Map(
      SystemColumns.id -> SoQLID,
      SystemColumns.createdAt -> SoQLFixedTimestamp,
      SystemColumns.updatedAt -> SoQLFixedTimestamp
    ), SystemColumns.id)

    val columnAdder = new ColumnAdder(highlevel, typeContext.nameFromType, StandardDatasetMapLimits.maximumPhysicalColumnBaseLength)

    val primaryKeySetter = new PrimaryKeySetter(highlevel)

    val upserter = new Upserter(highlevel)

    val publisher = new Publisher(highlevel)

    val workingCopyCreator = new WorkingCopyCreator(highlevel)

    // Above this can be re-used for every query

    val user = "robertm"

    try { datasetCreator.createDataset("crimes", user).unsafePerformIO() }
    catch { case _: DatasetAlreadyExistsException => /* pass */ }
    using(new CSVIterator(new File("/home/robertm/chicagocrime.csv"))) { it =>
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
        (ci, typeContext.typeFromName(ci.typeName))
      }.toMap
      primaryKeySetter.makePrimaryKey("crimes", "ID", user).unsafePerformIO()
      val start = System.nanoTime()
      upserter.upsert("crimes", user) { _ =>
        noopManagement(it.take(10).map(transformToRow(schema, headers, _)).map(Right(_)))
      }.unsafePerformIO()
      val end = System.nanoTime()
      println(s"Upsert took ${(end - start) / 1000000L}ms")
      publisher.publish("crimes", user).unsafePerformIO()
      workingCopyCreator.copyDataset("crimes", user, copyData = true).unsafePerformIO()
    }
  } finally {
    executor.shutdown()
  }

  def noopManagement[T](t: T): Managed[T] =
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
