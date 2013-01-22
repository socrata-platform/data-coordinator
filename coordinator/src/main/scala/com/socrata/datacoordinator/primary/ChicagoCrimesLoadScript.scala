package com.socrata.datacoordinator.primary

import scala.collection.JavaConverters._

import java.sql.{Connection, DriverManager}
import java.util.concurrent.Executors

import org.joda.time.{DateTimeZone, DateTime}
import com.rojoma.simplearm.{SimpleArm, Managed}
import com.rojoma.simplearm.util._
import com.google.protobuf.{CodedOutputStream, CodedInputStream}

import com.socrata.soql.types._
import com.socrata.id.numeric.{IdProvider, FibonacciIdProvider, InMemoryBlockIdProvider}

import com.socrata.datacoordinator.common.soql._
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.util._
import com.socrata.datacoordinator.manifest.TruthManifest
import com.socrata.datacoordinator.truth._
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresGlobalLog, PostgresDatasetMap}
import com.socrata.datacoordinator.manifest.sql.SqlTruthManifest
import com.socrata.datacoordinator.truth.loader.sql._
import com.socrata.datacoordinator.truth.sql.{DatabasePopulator, SqlColumnRep}
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.{Row, MutableRow}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.common.sql.{PostgresSqlLoaderProvider, AbstractSqlLoaderProvider}
import org.joda.time.format.DateTimeFormat
import java.io.Closeable
import scala.Tuple2

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

  val executor = Executors.newCachedThreadPool()

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

  try {
    val typeContext = SoQLTypeContext

    def rowCodecFactory(): RowLogCodec[Any] = SoQLRowLogCodec

    trait RepFactory {
      def base: String
      def rep(columnBase: String): SqlColumnRep[SoQLType, Any]
    }

    def rep(typ: SoQLType) = new RepFactory {
      val base = typ.toString.take(3)
      def rep(columnBase: String) = SoQLRep.repFactories(typ)(columnBase)
    }

    val soqlRepFactory = SoQLRep.repFactories.keys.foldLeft(Map.empty[SoQLType, RepFactory]) { (acc, typ) =>
      acc + (typ -> rep(typ))
    }

    def genericRepFor(columnInfo: ColumnInfo): SqlColumnRep[SoQLType, Any] =
      soqlRepFactory(typeContext.typeFromName(columnInfo.typeName)).rep(columnInfo.physicalColumnBase)

    val mutator: DatabaseMutator[SoQLType, Any] = new DatabaseMutator[SoQLType, Any] {
      class PoNT(val conn: Connection) extends ProviderOfNecessaryThings {
        val now: DateTime = DateTime.now()
        val datasetMap: DatasetMap = new PostgresDatasetMap(conn)

        def datasetLog(ds: DatasetInfo): Logger[Any] = new SqlLogger[Any](
          conn,
          ds.logTableName,
          rowCodecFactory
        )

        val globalLog: GlobalLog = new PostgresGlobalLog(conn)
        val truthManifest: TruthManifest = new SqlTruthManifest(conn)

        def physicalColumnBaseForType(typ: SoQLType): String =
          soqlRepFactory(typ).base

        def schemaLoader(version: VersionInfo, logger: Logger[Any]): SchemaLoader =
          new RepBasedSqlSchemaLoader[SoQLType, Any](conn, logger, genericRepFor)

        def nameForType(typ: SoQLType): String = typeContext.nameFromType(typ)

        def rawDataLoader(table: VersionInfo, schema: ColumnIdMap[ColumnInfo], logger: Logger[Any], idProvider: IdProvider): Loader[Any] = {
          val lp = new AbstractSqlLoaderProvider(conn, idProvider, executor, typeContext) with PostgresSqlLoaderProvider[SoQLType, Any]
          lp(table, schema, rowPreparer(schema), logger, genericRepFor)
        }

        def dataLoader(table: VersionInfo, schema: ColumnIdMap[ColumnInfo], logger: Logger[Any], idProvider: IdProvider): Managed[Loader[Any]] =
          managed(rawDataLoader(table, schema, logger, idProvider))

        def delogger(dataset: DatasetInfo) = new SqlDelogger[Any](conn, dataset.logTableName, rowCodecFactory)

        def rowPreparer(schema: ColumnIdMap[ColumnInfo]) =
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
      }

      def withTransaction[T]()(f: ProviderOfNecessaryThings => T): T = {
        for {
          conn <- managed(DriverManager.getConnection(url, username, pwd))
        } yield {
          conn.setAutoCommit(false)
          try {
            val result = f(new PoNT(conn))
            conn.commit()
            result
          } finally {
            conn.rollback()
          }
        }
      }

      def withSchemaUpdate[T](datasetId: String, user: String)(f: SchemaUpdate => T): T =
        withTransaction() { pontRaw =>
          val pont = pontRaw.asInstanceOf[PoNT]
          object Operations extends SchemaUpdate {
            val now: DateTime = pont.now
            val datasetMap = pont.datasetMap
            val datasetInfo = datasetMap.datasetInfo(datasetId).getOrElse(sys.error("no such dataset")) // TODO: Real error
            val tableInfo = datasetMap.latest(datasetInfo)
            val datasetLog = pont.datasetLog(datasetInfo)

            val schemaLoader: SchemaLoader = pont.schemaLoader(tableInfo, datasetLog)
            def datasetContentsCopier = new RepBasedSqlDatasetContentsCopier(pont.conn, datasetLog, genericRepFor)
          }

          val result = f(Operations)
          Operations.datasetLog.endTransaction() foreach { version =>
            pont.truthManifest.updateLatestVersion(Operations.datasetInfo, version)
            pont.globalLog.log(Operations.datasetInfo, version, pont.now, user)
          }
          result
        }

      def withDataUpdate[T](datasetId: String, user: String)(f: DataUpdate => T): T =
        withTransaction() { pontRaw =>
          val pont = pontRaw.asInstanceOf[PoNT]
          class Operations extends DataUpdate with Closeable {
            val now: DateTime = pont.now
            val datasetMap = pont.datasetMap
            val datasetInfo = datasetMap.datasetInfo(datasetId).getOrElse(sys.error("No such dataset?")) // TODO: better error
            val tableInfo = datasetMap.latest(datasetInfo)
            val datasetLog = pont.datasetLog(datasetInfo)

            val schema = datasetMap.schema(tableInfo)
            val rowIdProvider = new RowIdProvider(datasetInfo.nextRowId)
            val dataLoader = pont.rawDataLoader(tableInfo, schema, datasetLog, rowIdProvider)

            def close() {
              dataLoader.close()
            }
          }

          using(new Operations) { operations =>
            val result = f(operations)
            operations.datasetMap.updateNextRowId(operations.datasetInfo, operations.rowIdProvider.finish())
            operations.datasetLog.endTransaction() foreach { version =>
              pont.truthManifest.updateLatestVersion(operations.datasetInfo, version)
              pont.globalLog.log(operations.datasetInfo, version, pont.now, user)
            }
            result
          }
        }
    }

    val datasetCreator = new DatasetCreator(mutator, Map(
      SystemColumns.id -> SoQLID,
      SystemColumns.createdAt -> SoQLFixedTimestamp,
      SystemColumns.updatedAt -> SoQLFixedTimestamp
    ), SystemColumns.id)

    val columnAdder = new ColumnAdder(mutator)

    val primaryKeySetter = new PrimaryKeySetter(mutator)

    val upserter = new Upserter(mutator)

    // Everything above this point can be re-used for every operation

    using(DriverManager.getConnection(url, username, pwd)) { conn =>
      conn.setAutoCommit(false)
      DatabasePopulator.populate(conn)
      conn.commit()
    }

    val user = "robertm"

    try { datasetCreator.createDataset("crimes", user) }
    catch { case _: DatasetAlreadyExistsException => /* pass */ }
    using(loadCSV("/home/robertm/chicagocrime.csv")) { it =>
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
      val schema = columnAdder.addToSchema("crimes", headers.map { x => x -> types(x) }.toMap, user).mapValues { ci =>
        (ci, typeContext.typeFromName(ci.typeName))
      }.toMap
      primaryKeySetter.makePrimaryKey("crimes", "ID", user)
      val start = System.nanoTime()
      upserter.upsert("crimes", user) { _ =>
        noopManagement(it.map(transformToRow(schema, headers, _)).map(Right(_)))
      }
      val end = System.nanoTime()
      println(s"Upsert took ${(end - start) / 1000000L}ms")
    }
    // columnAdder.addToSchema("crimes", Map("id" -> SoQLText, "penalty" -> SoQLText), user)
    // primaryKeySetter.makePrimaryKey("crimes", "id", user)
    // loadRows("crimes", upserter, user)
    // loadRows2("crimes", upserter, user)

//    mutator.withTransaction() { mutator =>
//      val t = mutator.datasetMapReader.datasetInfo("crimes").getOrElse(sys.error("No crimes db?"))
//      val delogger = mutator.delogger(t)
//
//      def pt(n: Long) = using(delogger.delog(n)) { it =>
//        it/*.filterNot(_.isInstanceOf[Delogger.RowDataUpdated[_]])*/.foreach { ev => println(n + " : " + ev) }
//      }
//
//      (1L to 6) foreach (pt)
//    }
  } finally {
    executor.shutdown()
  }

  def noopManagement[T](t: T): Managed[T] =
    new SimpleArm[T] {
      def flatMap[B](f: (T) => B): B = f(t)
    }

  def loadRows(ds: String, upserter: Upserter[SoQLType, Any], user: String) {
    upserter.upsert(ds, user) { schema =>
      val byName = RotateSchema(schema)
      noopManagement(Iterator[Either[Any, Row[Any]]](
        Right(Row(byName("id").systemId -> "robbery", byName("penalty").systemId -> "short jail term")),
        Right(Row(byName("id").systemId -> "murder", byName("penalty").systemId -> "long jail term"))
      ))
    }
  }

  def loadRows2(ds: String, upserter: Upserter[SoQLType, Any], user: String) {
    upserter.upsert(ds, user) { schema =>
      val byName = RotateSchema(schema)
      noopManagement(Iterator[Either[Any, Row[Any]]](
        Right(Row(byName("id").systemId -> "murder", byName("penalty").systemId -> "DEATH")),
        Left("robbery")
      ))
    }
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

  def loadCSV(filename: String, skip: Int = 0): CloseableIterator[IndexedSeq[String]] = new CloseableIterator[IndexedSeq[String]] {
    import au.com.bytecode.opencsv._
    import java.io._
    val reader = new FileReader(filename)

    lazy val it = locally {
      val r = new CSVReader(reader)
      def loop(idx: Int = 1): Stream[Array[String]] = {
        r.readNext() match {
          case null => Stream.empty
          case row => row #:: loop(idx + 1)
        }
      }
      loop().iterator
    }

    def hasNext = it.hasNext
    def next() = it.next()

    def close() {
      reader.close()
    }
  }
}
