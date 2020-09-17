package com.socrata.datacoordinator.truth.metadata.sql

import java.sql.{Connection, DriverManager, SQLException}

import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.id.{ColumnId, RollupName, UserColumnId}
import com.socrata.datacoordinator.truth.metadata.{CopyPair, _}
import com.socrata.datacoordinator.truth.migration.Migration
import com.socrata.datacoordinator.util.NoopTimingReport
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.environment.{ColumnName, TypeName}
import org.scalatest.{BeforeAndAfterAll, FunSuite, MustMatchers}

import scala.concurrent.duration.Duration

/**
  * Tests the Postgres implementation of DatasetMapWriter
  *   _______________________________________
  *  / Beware! Unlike the rest of our unit   \
  *  | tests which use H2, this test runs    |
  *  | against a test postgres database      |
  *  \ datacoordinator_test with user blist. /
  *   ---------------------------------------
  *        \                    / \  //\
  *         \    |\___/|      /   \//  \\
  *              /0  0  \__  /    //  | \ \
  *             /     /  \/_/    //   |  \  \
  *             @_^_@'/   \/_   //    |   \   \
  *             //_^_/     \/_ //     |    \    \
  *          ( //) |        \///      |     \     \
  *        ( / /) _|_ /   )  //       |      \     _\
  *      ( // /) '/,_ _ _/  ( ; -.    |    _ _\.-~        .-~~~^-.
  *    (( / / )) ,-{        _      `-.|.-~-.           .~         `.
  *   (( // / ))  '/\      /                 ~-. _ .-~      .-~^-.  \
  *   (( /// ))      `.   {            }                   /      \  \
  *    (( / ))     .----~-.\        \-'                 .~         \  `. \^-.
  *               ///.----..>        \             _ -~             `.  ^-`  ^-_
  *                 ///-._ _ _ _ _ _ _}^ - - - - ~                     ~-- ,.-~
  *                                                                    /.-~
  */
class PostgresDatasetMapWriterTest extends FunSuite with PostgresDatasetMapWriterIndexDirectiveTest with MustMatchers with BeforeAndAfterAll {
  def c(s: String) = new UserColumnId(s)
  def t(s: String) = TypeName(s)
  def fn(s: String) = Some(ColumnName(s))

  val noopKeyGen = () => new Array[Byte](0)
  val noopTypeNamespace = new TypeNamespace[TypeName] {
    def nameForType(typ: TypeName): String = typ.name

    def typeForName(datasetInfo: DatasetInfo, typeName: String): TypeName = TypeName(typeName)

    def typeForUserType(typeName: TypeName): Option[TypeName] = Some(typeName)

    def userTypeForType(typ: TypeName): TypeName = typ
  }
  val ZeroID = 0L
  val ZeroVersion = 0L

  val resourcName = Some("_abcd-1234")

  val dbName = "datacoordinator_test"

  def withPostgresDb(sql: String): Unit = {
    using(DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "blist", "blist")) { conn =>
      conn.setAutoCommit(true)
      using(conn.createStatement()) { stmt =>
        stmt.execute(sql)
      }
    }
  }

  def populateDatabase(conn: Connection) {
    Migration.migrateDb(conn)
  }

  def withDb[T]()(f: (Connection) => T): T = {
    using(DriverManager.getConnection(s"jdbc:postgresql://localhost:5432/$dbName", "blist", "blist")) { conn =>
      conn.setAutoCommit(false)
      f(conn)
    }
  }

  override def beforeAll(): Unit = {
    withPostgresDb(s"DROP DATABASE IF EXISTS $dbName; CREATE DATABASE $dbName;")
    withDb() { conn => populateDatabase(conn) }
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    withPostgresDb(s"DROP DATABASE IF EXISTS $dbName;")
    super.afterAll()
  }

  def count(conn: Connection, table: String, where: String = null): Int = {
    for {
      stmt <- managed(conn.createStatement())
      rs <- managed(stmt.executeQuery("SELECT count(*) FROM " + table + (if(where != null) " WHERE " + where else "")))
    } {
      rs.next()
      rs.getInt(1)
    }
  }

  test("Can create a table") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi = tables.create("en_US", resourcName)

      vi.datasetInfo.tableBase must be ("t" + vi.datasetInfo.systemId.underlying)
      vi.lifecycleStage must be (LifecycleStage.Unpublished)
      vi.copyNumber must be (1)

      tables.datasetInfo(vi.datasetInfo.systemId, Duration.Inf) must equal (Some(vi.datasetInfo))
      tables.unpublished(vi.datasetInfo) must equal (Some(vi))
    }
  }

  test("Can add a column to a table") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi = tables.create("en_US", resourcName)
      val ci = tables.addColumn(vi, c("col1"), fn("co1"), t("typ"), "colbase")

      ci.copyInfo must equal (vi)
      ci.userColumnId must be (c("col1"))
      ci.typ must be (t("typ"))
      ci.physicalColumnBaseBase must be ("colbase")
      ci.isUserPrimaryKey must be (false)

      tables.schema(vi) must equal (ColumnIdMap(ci.systemId -> ci))
    }
  }

  test("Can make a column a primary key") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi = tables.create("en_US", resourcName)
      val ci = tables.addColumn(vi, c("col1"), fn("co1"), t("typ"), "colbase")

      tables.setUserPrimaryKey(ci)

      tables.schema(vi) must equal (ColumnIdMap(ci.systemId -> ci.copy(isUserPrimaryKey = true)(noopTypeNamespace, null)))
    }
  }

  test("Can add a second column to a table") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi = tables.create("en_US", resourcName)
      val ci1 = tables.addColumn(vi, c("col1"), fn("co1"), t("typ"), "colbase")
      val ci2 = tables.addColumn(vi, c("col2"), fn("co2"), t("typ2"), "colbase2")

      ci2.copyInfo must equal (vi)
      ci2.userColumnId must be (c("col2"))
      ci2.typ must be (t("typ2"))
      ci2.physicalColumnBaseBase must be ("colbase2")
      ci2.isUserPrimaryKey must be (false)

      tables.schema(vi) must equal (ColumnIdMap(ci1.systemId -> ci1, ci2.systemId -> ci2))
    }
  }

  test("Cannot have multiple primary keys") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi = tables.create("en_US", resourcName)
      val ci1 = tables.addColumn(vi, c("col1"), fn("co1"), t("typ"), "colbase")
      val ci2 = tables.addColumn(vi, c("col2"), fn("co2"), t("typ2"), "colbase2")

      tables.setUserPrimaryKey(ci1)

      an [SQLException] must be thrownBy (tables.setUserPrimaryKey(ci2))
    }
  }


  test("Can clear a user primary key and re-seat it") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi = tables.create("en_US", resourcName)
      val ci1 = tables.addColumn(vi, c("col1"), fn("co1"), t("typ"), "colbase")
      val ci2 = tables.addColumn(vi, c("col2"), fn("co2"), t("typ2"), "colbase2")

      val pk = tables.setUserPrimaryKey(ci1)
      tables.clearUserPrimaryKey(pk)
      tables.setUserPrimaryKey(ci2)

      tables.schema(vi) must equal (ColumnIdMap(ci1.systemId -> ci1, ci2.systemId -> ci2.copy(isUserPrimaryKey = true)(noopTypeNamespace, null)))
    }
  }

  test("Cannot add the same column twice") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi = tables.create("en_US", resourcName)
      tables.addColumn(vi, c("col1"), fn("co1"), t("typ"), "colbase")

      an [ColumnAlreadyExistsException] must be thrownBy (tables.addColumn(vi, c("col1"), fn("co1"), t("typ2"), "colbase2"))
    }
  }

  test("Can publish the initial working copy") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi1 = tables.create("en_US", resourcName)
      val vi2 = tables.publish(vi1)
      vi2 must equal ((vi1.copy(lifecycleStage = LifecycleStage.Published)(null), None))

      tables.published(vi2._1.datasetInfo) must equal (Some(vi2._1))
      tables.unpublished(vi1.datasetInfo) must be (None)
    }
  }

  test("Can drop a column") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi = tables.create("en_US", resourcName)
      val c1 = tables.addColumn(vi, c("col1"), fn("co1"), t("typ1"), "pcol1")
      val c2 = tables.addColumn(vi, c("col2"), fn("co2"), t("typ2"), "pcol2")

      tables.dropColumn(c2)

      tables.schema(vi) must equal (ColumnIdMap(c1.systemId -> c1))
    }
  }

  test("Can make a working copy") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi1 = tables.publish(tables.create("en_US", resourcName))
      val ci1 = tables.addColumn(vi1._1, c("col1"), fn("co1"), t("typ"), "colbase")
      val ci2 = tables.addColumn(vi1._1, c("col2"), fn("co2"), t("typ2"), "colbase2")

      tables.unpublished(vi1._1.datasetInfo) must be (None)

      val Right(CopyPair(vi1a, vi2)) = tables.ensureUnpublishedCopy(vi1._1.datasetInfo)
      vi1a must equal (vi1._1)

      val schema1 = tables.schema(vi1._1)
      val schema2 = tables.schema(vi2)
      schema1.values.toSeq.map(_.unanchored).sortBy(_.userColumnId) must equal (schema2.values.toSeq.map(_.unanchored).sortBy(_.userColumnId))
    }
  }

  test("Cannot drop a published version") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi1 = tables.create("en_US", resourcName)
      val vi2 = tables.publish(vi1)

      vi2._1.lifecycleStage must be (LifecycleStage.Published)
      an [CopyInWrongStateForDropException] must be thrownBy { tables.dropCopy(vi2._1) }
    }
  }

  test("Cannot drop the initial version when it's still unpublished") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi = tables.create("en_US", resourcName)
      vi.lifecycleStage must be (LifecycleStage.Unpublished)
      an [CannotDropInitialWorkingCopyException] must be thrownBy { tables.dropCopy(vi) }
    }
  }

  test("Can drop a non-initial unpublished version") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi1 = tables.create("en_US", resourcName)
      val vi2 = tables.publish(vi1)
      val Right(CopyPair(_, vi3)) = tables.ensureUnpublishedCopy(vi2._1.datasetInfo)
      tables.unpublished(vi1.datasetInfo) must equal (Some(vi3))

      tables.dropCopy(vi3)

      tables.unpublished(vi1.datasetInfo) must be (None)
    }
  }

  test("Column IDs are allocated into the first gap if one exists") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi = tables.create("en_US", resourcName)
      tables.addColumnWithId(new ColumnId(0), vi, c("col0"), fn("co0"), t("typ0"), "t")
      tables.addColumnWithId(new ColumnId(2), vi, c("col2"), fn("co2"), t("typ2"), "t")
      tables.addColumn(vi, c("col1"), fn("co1"), t("typ1"), "t").systemId must equal (new ColumnId(1))
    }
  }

  test("Column IDs are allocated as 0 if it doesn't exist") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi = tables.create("en_US", resourcName)
      tables.addColumnWithId(new ColumnId(1), vi, c("col1"), fn("co1"), t("typ1"), "t")
      tables.addColumnWithId(new ColumnId(2), vi, c("col2"), fn("co2"), t("typ2"), "t")
      tables.addColumn(vi, c("col0"), fn("co0"), t("typ0"), "t").systemId must equal (new ColumnId(0))
    }
  }

  test("Column IDs are allocated at the end if there are no gaps") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi = tables.create("en_US", resourcName)
      tables.addColumnWithId(new ColumnId(0), vi, c("col0"), fn("co0"), t("typ0"), "t")
      tables.addColumnWithId(new ColumnId(1), vi, c("col1"), fn("co1"), t("typ1"), "t")
      tables.addColumn(vi, c("col2"), fn("co2"), t("typ2"), "t").systemId must equal (new ColumnId(2))
    }
  }

  test("Adding a column to a table does not use IDs from this table or the previous version") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi0 = tables.create("en_US", resourcName)
      val ci0 = tables.addColumn(vi0, c("col0"), fn("co0"), t("typ0"), "base0")
      val ci1 = tables.addColumn(vi0, c("col1"), fn("co1"), t("typ1"), "base1")
      val vi1 = tables.ensureUnpublishedCopy(tables.publish(vi0)._1.datasetInfo).right.get.newCopyInfo
      val ci2 = tables.addColumn(vi1, c("col2"), fn("co2"), t("typ2"), "base2")
      tables.dropColumn(tables.schema(vi1)(new ColumnId(1)))
      val ci3 = tables.addColumn(vi1, c("col3"), fn("co3"), t("typ3"), "base3")

      ci3.systemId must be (new ColumnId(3))
    }
  }

  test("Can delete a table entirely") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi1 = tables.create("en_US", resourcName)
      tables.addColumn(vi1, c("col1"), fn("co1"), t("typ1"), "pcol1")
      tables.addColumn(vi1, c("col2"), fn("co2"), t("typ2"), "pcol2")

      (1 to 5).foldLeft(vi1) { (vi, _) =>
        val vi2 = tables.publish(vi)
        tables.ensureUnpublishedCopy(vi2._1.datasetInfo).right.get.newCopyInfo
      }

      // ok, there should be six copies now, which means twelve columns....
      count(conn, "column_map") must equal (12)
      count(conn, "copy_map") must equal (6)
      count(conn, "dataset_map") must equal (1)

      tables.delete(vi1.datasetInfo)

      count(conn, "column_map") must equal (0)
      count(conn, "copy_map") must equal (0)
      count(conn, "dataset_map") must equal (0)
    }
  }

  val rollupName = new RollupName("my rollup")
  val rollupNames = Seq(rollupName, new RollupName("my rollup 2"))
  val rollupSoql = "select clown_type, count(*) group by clown_type"
  val rollupSoql2 = "select clown_volume, count(*) group by clown_volume"

  test("Can create and update rollup metadata") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi1 = tables.create("en_US", resourcName)
      def rollupCount = count(conn, "rollup_map", s"name = '${rollupName.underlying}' AND copy_system_id=${vi1.systemId.underlying}")

      rollupCount must be (0)
      val createdRollup = tables.createOrUpdateRollup(vi1, rollupName, rollupSoql)
      createdRollup.copyInfo must equal (vi1)
      createdRollup.name must equal (rollupName)
      createdRollup.soql must equal (rollupSoql)
      rollupCount must be (1)

      val updatedRollup = tables.createOrUpdateRollup(vi1, rollupName, rollupSoql2)
      updatedRollup.copyInfo must equal (vi1)
      updatedRollup.name must equal (rollupName)
      updatedRollup.soql must equal (rollupSoql2)
      rollupCount must be (1)
    }
  }

  test("Can lookup rollup metadata") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi1 = tables.create("en_US", resourcName)

      tables.rollup(vi1, rollupName) must be (None)
      tables.createOrUpdateRollup(vi1, rollupName, rollupSoql)

      tables.rollup(vi1, rollupName) match {
        case Some(lookedupRollup) =>
          lookedupRollup.copyInfo must equal (vi1)
          lookedupRollup.name must equal (rollupName)
          lookedupRollup.soql must equal (rollupSoql)
        case None =>
          fail("Couldn't lookup rollup")
      }
    }
  }

  test("Can drop rollup metadata") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi1 = tables.create("en_US", resourcName)

      tables.rollup(vi1, rollupName) must be (None)
      tables.createOrUpdateRollup(vi1, rollupName, rollupSoql)
      tables.rollup(vi1, rollupName) must not be (None)
      tables.dropRollup(vi1, Some(rollupName))
      tables.rollup(vi1, rollupName) must be (None)
    }
  }

  test("Can drop multiple rollup metadata") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi1 = tables.create("en_US", resourcName)

      tables.rollup(vi1, rollupName) must be (None)
      rollupNames.foreach(name => tables.createOrUpdateRollup(vi1, name, rollupSoql))
      rollupNames.foreach(name => tables.rollup(vi1, name) must not be (None))
      tables.dropRollup(vi1, None)
      rollupNames.foreach(name => tables.rollup(vi1, name) must be (None))
    }
  }

  test("Dropping dataset drops all rollup metadata") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi1 = tables.create("en_US", resourcName)

      tables.publish(vi1)
      tables.ensureUnpublishedCopy(vi1.datasetInfo) match {
        case Left(vi2) => fail("Didn't create a new copy?")
        case Right(CopyPair(_, vi2)) =>
          vi2.systemId must not equal (vi1.systemId)
          rollupNames.foreach(name => tables.rollup(vi2, name) must be (None))
          rollupNames.foreach(name => tables.createOrUpdateRollup(vi2, name, rollupSoql))
          rollupNames.foreach(name => tables.rollup(vi2, name) must not be (None))

          tables.delete(vi2.datasetInfo)
          rollupNames.foreach(name => tables.rollup(vi2, name) must be (None))
      }
    }
  }
}
