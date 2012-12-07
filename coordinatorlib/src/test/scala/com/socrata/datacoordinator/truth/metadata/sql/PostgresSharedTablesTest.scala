package com.socrata.datacoordinator.truth.metadata.sql

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.matchers.MustMatchers
import java.sql.{SQLException, Connection, DriverManager}
import com.socrata.datacoordinator.truth.sql.DatabasePopulator
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.metadata.LifecycleStage

class PostgresSharedTablesTest extends FunSuite with MustMatchers with BeforeAndAfterAll {
  override def beforeAll() {
    // In Java 6 (sun and open) driver registration is not thread-safe!
    // So since SBT will run these tests in parallel, sometimes one of the
    // first tests to run will randomly fail.  By forcing the driver to
    // be loaded up front we can avoid this.
    Class.forName("org.postgresql.Driver")
  }

  def populateDatabase(conn: Connection) {
    val populator = new DatabasePopulator
    val sql = populator.metadataTablesCreate(20, 20, 20, 20, 20, 20)
    using(conn.createStatement()) { stmt =>
      stmt.execute(sql)
    }
  }

  def withDb[T]()(f: Connection => T): T = {
    using(DriverManager.getConnection("jdbc:postgresql://localhost:5432/blist_test", "blist", "blist")) { conn =>
      conn.setAutoCommit(false)
      populateDatabase(conn)
      f(conn)
    }
  }

  test("Can create a table") {
    withDb() { conn =>
      val tables = new PostgresSharedTables(conn)
      val vi = tables.create("hello", "world")

      vi.tableInfo.datasetId must be ("hello")
      vi.tableInfo.tableBase must be ("world")
      vi.lifecycleStage must be (LifecycleStage.Unpublished)
      vi.lifecycleVersion must be (1)

      tables.tableInfo("hello") must equal (Some(vi.tableInfo))
      tables.unpublished(vi.tableInfo) must equal (Some(vi))
    }
  }

  test("Can add a column to a table") {
    withDb() { conn =>
      val tables = new PostgresSharedTables(conn)
      val vi = tables.create("hello", "world")
      val ci = tables.addColumn(vi, "col1", "typ", "colbase")

      ci.versionInfo must equal (vi)
      ci.logicalName must be ("col1")
      ci.typeName must be ("typ")
      ci.physicalColumnBase must be ("colbase")
      ci.isPrimaryKey must be (false)

      tables.schema(vi) must equal (Map("col1" -> ci))
    }
  }

  test("Can make a column a primary key") {
    withDb() { conn =>
      val tables = new PostgresSharedTables(conn)
      val vi = tables.create("hello", "world")
      val ci = tables.addColumn(vi, "col1", "typ", "colbase")

      tables.setUserPrimaryKey(ci)

      tables.schema(vi) must equal (Map("col1" -> ci.copy(isPrimaryKey = true)))
    }
  }

  test("Can add a second column to a table") {
    withDb() { conn =>
      val tables = new PostgresSharedTables(conn)
      val vi = tables.create("hello", "world")
      val ci1 = tables.addColumn(vi, "col1", "typ", "colbase")
      val ci2 = tables.addColumn(vi, "col2", "typ2", "colbase2")

      ci2.versionInfo must equal (vi)
      ci2.logicalName must be ("col2")
      ci2.typeName must be ("typ2")
      ci2.physicalColumnBase must be ("colbase2")
      ci2.isPrimaryKey must be (false)

      tables.schema(vi) must equal (Map("col1" -> ci1, "col2" -> ci2))
    }
  }

  test("Cannot have multiple primary keys") {
    withDb() { conn =>
      val tables = new PostgresSharedTables(conn)
      val vi = tables.create("hello", "world")
      val ci1 = tables.addColumn(vi, "col1", "typ", "colbase")
      val ci2 = tables.addColumn(vi, "col2", "typ2", "colbase2")

      tables.setUserPrimaryKey(ci1)

      evaluating(tables.setUserPrimaryKey(ci2)) must produce [SQLException]
    }
  }


  test("Can clear a user primary key and re-seat it") {
    withDb() { conn =>
      val tables = new PostgresSharedTables(conn)
      val vi = tables.create("hello", "world")
      val ci1 = tables.addColumn(vi, "col1", "typ", "colbase")
      val ci2 = tables.addColumn(vi, "col2", "typ2", "colbase2")

      tables.setUserPrimaryKey(ci1)
      tables.clearUserPrimaryKey(vi)
      tables.setUserPrimaryKey(ci2)

      tables.schema(vi) must equal (Map("col1" -> ci1, "col2" -> ci2.copy(isPrimaryKey = true)))
    }
  }

  test("Cannot add the same column twice") {
    withDb() { conn =>
      val tables = new PostgresSharedTables(conn)
      val vi = tables.create("hello", "world")
      tables.addColumn(vi, "col1", "typ", "colbase")

      evaluating(tables.addColumn(vi, "col1", "typ2", "colbase2")) must produce [SQLException]
    }
  }

  test("Can publish the initial working copy") {
    withDb() { conn =>
      val tables = new PostgresSharedTables(conn)
      val vi1 = tables.create("hello", "world")
      val vi2 = tables.publish(vi1.tableInfo)
      vi2 must equal (Some(vi1.copy(lifecycleStage = LifecycleStage.Published)))

      tables.published(vi2.get.tableInfo) must equal (vi2)
      tables.unpublished(vi1.tableInfo) must be (None)
    }
  }
}
