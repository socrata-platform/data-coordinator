package com.socrata.datacoordinator.main

import scala.collection.JavaConverters._

import java.sql.DriverManager

import org.joda.time.{DateTimeZone, DateTime}
import com.rojoma.simplearm.{SimpleArm, Managed}
import com.rojoma.simplearm.util._
import com.google.protobuf.{CodedOutputStream, CodedInputStream}

import com.socrata.soql.types._
import com.socrata.id.numeric.{FibonacciIdProvider, InMemoryBlockIdProvider}

import com.socrata.datacoordinator.main.soql.{SoQLRep, SoQLNullValue, SystemColumns}
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.loader.{RowPreparer, SchemaLoader, Loader, Logger}
import com.socrata.datacoordinator.util.{IdProviderPoolImpl, IdProviderPool}
import com.socrata.datacoordinator.manifest.TruthManifest
import com.socrata.datacoordinator.truth._
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresGlobalLog, PostgresDatasetMapWriter, PostgresDatasetMapReader}
import com.socrata.datacoordinator.manifest.sql.SqlTruthManifest
import com.socrata.datacoordinator.truth.loader.sql.{SqlLoader, PostgresRepBasedDataSqlizer, SqlLogger, RepBasedSchemaLoader}
import com.socrata.datacoordinator.truth.sql.{DatabasePopulator, SqlColumnRep}
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.{Row, MutableRow}
import com.socrata.soql.environment.TypeName
import java.util.concurrent.Executors
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.main.sql.{PostgresSqlLoaderProvider, AbstractSqlLoaderProvider}

object ChicagoCrimesLoadScript extends App {
  val executor = Executors.newCachedThreadPool()

  try {
    val typeContext: TypeContext[SoQLType, Any] = new TypeContext[SoQLType, Any] {
      def isNull(value: Any): Boolean = SoQLNullValue == value

      def makeValueFromSystemId(id: RowId): Any = id

      def makeSystemIdFromValue(id: Any): RowId = id.asInstanceOf[RowId]

      def nullValue: Any = SoQLNullValue

      def typeFromName(name: String): SoQLType = SoQLType.typesByName(TypeName(name))

      def nameFromType(typ: SoQLType): String = typ.toString

      def makeIdMap[T](idColumnType: SoQLType): RowUserIdMap[Any, T] =
        if(idColumnType == SoQLText) {
          new RowUserIdMap[Any, T] {
            val map = new java.util.HashMap[String, (String, T)]

            def put(x: Any, v: T) {
              val s = x.asInstanceOf[String]
              map.put(s.toLowerCase, (s, v))
            }

            def apply(x: Any): T = {
              val s = x.asInstanceOf[String]
              val k = s.toLowerCase
              if(map.containsKey(k)) map.get(k)._2
              else throw new NoSuchElementException
            }

            def get(x: Any): Option[T] = {
              val s = x.asInstanceOf[String]
              val k = s.toLowerCase
              if(map.containsKey(k)) Some(map.get(k)._2)
              else None
            }

            def clear() {
              map.clear()
            }

            def contains(x: Any): Boolean = {
              val s = x.asInstanceOf[String]
              map.containsKey(s.toLowerCase)
            }

            def isEmpty: Boolean = map.isEmpty

            def size: Int = map.size

            def foreach(f: (Any, T) => Unit) {
              val it = map.values.iterator
              while(it.hasNext) {
                val (k, v) = it.next()
                f(k, v)
              }
            }

            def valuesIterator: Iterator[T] =
              map.values.iterator.asScala.map(_._2)
          }
        } else {
          new SimpleRowUserIdMap[Any, T]
        }
    }

    val idSource = new InMemoryBlockIdProvider(releasable = true)

    val providerPool: IdProviderPool = new IdProviderPoolImpl(idSource, new FibonacciIdProvider(_))

    def rowCodecFactory(): RowLogCodec[Any] = new IdCachingRowLogCodec[Any] {
      def rowDataVersion: Short = 0

      // fixme; it'd be much better to do this in a manner simular to how column reps work

      protected def writeValue(target: CodedOutputStream, v: Any) {
        v match {
          case l: RowId =>
            target.writeRawByte(0)
            target.writeInt64NoTag(l.underlying)
          case s: String =>
            target.writeRawByte(1)
            target.writeStringNoTag(s)
          case bd: BigDecimal =>
            target.writeRawByte(2)
            target.writeStringNoTag(bd.toString)
          case b: Boolean =>
            target.writeRawByte(3)
            target.writeBoolNoTag(b)
          case ts: DateTime =>
            target.writeRawByte(4)
            target.writeStringNoTag(ts.getZone.getID)
            target.writeInt64NoTag(ts.getMillis)
          case SoQLNullValue =>
            target.writeRawByte(-1)
        }
      }

      protected def readValue(source: CodedInputStream): Any =
        source.readRawByte() match {
          case 0 =>
            new RowId(source.readInt64())
          case 1 =>
            source.readString()
          case 2 =>
            BigDecimal(source.readString())
          case 3 =>
            source.readBool()
          case 4 =>
            val zone = DateTimeZone.forID(source.readString())
            new DateTime(source.readInt64(), zone)
          case -1 =>
            SoQLNullValue
        }
    }

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

    val mutator: DatabaseMutator[SoQLType, Any] = new DatabaseMutator[SoQLType, Any] {
      def withTransaction[T]()(f: (ProviderOfNecessaryThings) => T): T = {
        using(DriverManager.getConnection("jdbc:postgresql://localhost:5432/robertm", "blist", "blist")) { conn =>
          conn.setAutoCommit(false)
          try {
            object PoNT extends ProviderOfNecessaryThings {
              val now: DateTime = DateTime.now()
              val datasetMapReader: DatasetMapReader = new PostgresDatasetMapReader(conn)
              val datasetMapWriter: DatasetMapWriter = new PostgresDatasetMapWriter(conn)

              def datasetLog(ds: DatasetInfo): Logger[Any] = new SqlLogger[Any](
                conn,
                ds.logTableName,
                rowCodecFactory
              )

              val globalLog: GlobalLog = new PostgresGlobalLog(conn)
              val truthManifest: TruthManifest = new SqlTruthManifest(conn)
              val idProviderPool: IdProviderPool = providerPool

              def physicalColumnBaseForType(typ: SoQLType): String =
                soqlRepFactory(typ).base

              def genericRepFor(columnInfo: ColumnInfo): SqlColumnRep[SoQLType, Any] =
                soqlRepFactory(typeContext.typeFromName(columnInfo.typeName)).rep(columnInfo.physicalColumnBase)

              def schemaLoader(version: datasetMapWriter.VersionInfo, logger: Logger[Any]): SchemaLoader =
                new RepBasedSchemaLoader[SoQLType, Any](conn, logger) {
                  def repFor(columnInfo: DatasetMapWriter#ColumnInfo): SqlColumnRep[SoQLType, Any] =
                    genericRepFor(columnInfo)
                }

              def nameForType(typ: SoQLType): String = typeContext.nameFromType(typ)

              def dataLoader(table: VersionInfo, schema: ColumnIdMap[ColumnInfo], logger: Logger[Any]): Managed[Loader[Any]] = {
                val lp = new AbstractSqlLoaderProvider(conn, providerPool, executor, typeContext) with PostgresSqlLoaderProvider[SoQLType, Any]
                lp(table, schema, rowPreparer(schema), logger, genericRepFor)
              }

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
            val result = f(PoNT)
            conn.commit()
            result
          } finally {
            conn.rollback()
          }
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

    using(DriverManager.getConnection("jdbc:postgresql://localhost:5432/robertm", "blist", "blist")) { conn =>
      conn.setAutoCommit(false)
      DatabasePopulator.populate(conn)
      conn.commit()
    }

    val user = "robertm"

    datasetCreator.createDataset("crimes", user)
    columnAdder.addToSchema("crimes", Map("id" -> SoQLText, "penalty" -> SoQLText), user)
    primaryKeySetter.makePrimaryKey("crimes", "id", user)
    loadRows("crimes", upserter, user)
    loadRows2("crimes", upserter, user)
  } finally {
    executor.shutdown()
  }

  def noopManagement[T](t: T): Managed[T] =
    new SimpleArm[T] {
      def flatMap[B](f: (T) => B): B = f(t)
    }

  def loadRows(ds: String, upserter: Upserter[SoQLType, Any], user: String) {
    upserter.upsert(ds, user) { schema =>
      val idCol = schema.iterator.find { _._2.logicalName == "id" }.getOrElse(sys.error("No id column?"))._1
      val penaltyCol = schema.iterator.find { _._2.logicalName == "penalty" }.getOrElse(sys.error("No id column?"))._1
      noopManagement(Iterator[Either[Any, Row[Any]]](
        Right(Row(idCol -> "robbery", penaltyCol -> "short jail term")),
        Right(Row(idCol -> "murder", penaltyCol -> "long jail term"))
      ))
    }
  }

  def loadRows2(ds: String, upserter: Upserter[SoQLType, Any], user: String) {
    upserter.upsert(ds, user) { schema =>
      val idCol = schema.iterator.find { _._2.logicalName == "id" }.getOrElse(sys.error("No id column?"))._1
      val penaltyCol = schema.iterator.find { _._2.logicalName == "penalty" }.getOrElse(sys.error("No id column?"))._1
      noopManagement(Iterator[Either[Any, Row[Any]]](
        Left("robbery"),
        Right(Row(idCol -> "murder", penaltyCol -> "DEATH"))
      ))
    }
  }
}
