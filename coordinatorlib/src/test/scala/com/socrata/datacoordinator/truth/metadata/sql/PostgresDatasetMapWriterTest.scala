package com.socrata.datacoordinator.truth.metadata.sql

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.matchers.MustMatchers
import java.sql.{SQLException, Connection, DriverManager}
import com.socrata.datacoordinator.truth.sql.DatabasePopulator
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.metadata.LifecycleStage
import com.socrata.datacoordinator.util.collection.ColumnIdMap

class PostgresDatasetMapWriterTest extends FunSuite with MustMatchers with BeforeAndAfterAll {
  override def beforeAll() {
    // In Java 6 (sun and open) driver registration is not thread-safe!
    // So since SBT will run these tests in parallel, sometimes one of the
    // first tests to run will randomly fail.  By forcing the driver to
    // be loaded up front we can avoid this.
    Class.forName("org.postgresql.Driver")
  }

  def populateDatabase(conn: Connection) {
    val populator = new DatabasePopulator
    val sql = populator.metadataTablesCreate(20, 20, 20, 20, 20, 20, 20)
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

  def count(conn: Connection, table: String, where: String = null): Int = {
    for {
      stmt <- managed(conn.createStatement())
      rs <- managed(stmt.executeQuery("SELECT count(*) FROM " + table + (if(where != null) " WHERE " + where else "")))
    } yield {
      rs.next()
      rs.getInt(1)
    }
  }

  test("Can create a table") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn)
      val vi = tables.create("hello", "world")

      vi.datasetInfo.datasetId must be ("hello")
      vi.datasetInfo.tableBase must be ("world")
      vi.lifecycleStage must be (LifecycleStage.Unpublished)
      vi.lifecycleVersion must be (1)

      tables.datasetInfo("hello") must equal (Some(vi.datasetInfo))
      tables.unpublished(vi.datasetInfo) must equal (Some(vi))
    }
  }

  test("Can add a column to a table") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn)
      val vi = tables.create("hello", "world")
      val ci = tables.addColumn(vi, "col1", "typ", "colbase")

      ci.versionInfo must equal (vi)
      ci.logicalName must be ("col1")
      ci.typeName must be ("typ")
      ci.physicalColumnBase must be ("colbase")
      ci.isPrimaryKey must be (false)

      tables.schema(vi) must equal (ColumnIdMap(ci.systemId -> ci))
    }
  }

  test("Can make a column a primary key") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn)
      val vi = tables.create("hello", "world")
      val ci = tables.addColumn(vi, "col1", "typ", "colbase")

      tables.setUserPrimaryKey(ci)

      tables.schema(vi) must equal (ColumnIdMap(ci.systemId -> ci.copy(isPrimaryKey = true)))
    }
  }

  test("Can add a second column to a table") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn)
      val vi = tables.create("hello", "world")
      val ci1 = tables.addColumn(vi, "col1", "typ", "colbase")
      val ci2 = tables.addColumn(vi, "col2", "typ2", "colbase2")

      ci2.versionInfo must equal (vi)
      ci2.logicalName must be ("col2")
      ci2.typeName must be ("typ2")
      ci2.physicalColumnBase must be ("colbase2")
      ci2.isPrimaryKey must be (false)

      tables.schema(vi) must equal (ColumnIdMap(ci1.systemId -> ci1, ci2.systemId -> ci2))
    }
  }

  test("Cannot have multiple primary keys") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn)
      val vi = tables.create("hello", "world")
      val ci1 = tables.addColumn(vi, "col1", "typ", "colbase")
      val ci2 = tables.addColumn(vi, "col2", "typ2", "colbase2")

      tables.setUserPrimaryKey(ci1)

      evaluating(tables.setUserPrimaryKey(ci2)) must produce [SQLException]
    }
  }


  test("Can clear a user primary key and re-seat it") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn)
      val vi = tables.create("hello", "world")
      val ci1 = tables.addColumn(vi, "col1", "typ", "colbase")
      val ci2 = tables.addColumn(vi, "col2", "typ2", "colbase2")

      tables.setUserPrimaryKey(ci1)
      tables.clearUserPrimaryKey(vi)
      tables.setUserPrimaryKey(ci2)

      tables.schema(vi) must equal (ColumnIdMap(ci1.systemId -> ci1, ci2.systemId -> ci2.copy(isPrimaryKey = true)))
    }
  }

  test("Cannot add the same column twice") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn)
      val vi = tables.create("hello", "world")
      tables.addColumn(vi, "col1", "typ", "colbase")

      evaluating(tables.addColumn(vi, "col1", "typ2", "colbase2")) must produce [SQLException]
    }
  }

  test("Can publish the initial working copy") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn)
      val vi1 = tables.create("hello", "world")
      val vi2 = tables.publish(vi1)
      vi2 must equal (vi1.copy(lifecycleStage = LifecycleStage.Published))

      tables.published(vi2.datasetInfo) must equal (Some(vi2))
      tables.unpublished(vi1.datasetInfo) must be (None)
    }
  }

  test("Can drop a column") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn)
      val vi = tables.create("hello", "world")
      val c1 = tables.addColumn(vi, "col1", "typ1", "pcol1")
      val c2 = tables.addColumn(vi, "col2", "typ2", "pcol2")

      tables.dropColumn(c2)

      tables.schema(vi) must equal (ColumnIdMap(c1.systemId -> c1))
    }
  }

  test("Can make a working copy") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn)
      val vi1 = tables.publish(tables.create("hello", "world"))
      val ci1 = tables.addColumn(vi1, "col1", "typ", "colbase")
      val ci2 = tables.addColumn(vi1, "col2", "typ2", "colbase2")

      tables.unpublished(vi1.datasetInfo) must be (None)

      val vi2 = tables.ensureUnpublishedCopy(vi1.datasetInfo)

      // and columns get copied...
      val schema1 = tables.schema(vi1)
      val schema2 = tables.schema(vi2)

      schema1.values.toSeq.map(_.copy(versionInfo = null)).sortBy(_.logicalName) must equal (schema2.values.toSeq.map(_.copy(versionInfo = null)).sortBy(_.logicalName))
    }
  }

  test("Cannot drop a published version") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn)
      val vi1 = tables.create("hello", "world")
      val vi2 = tables.publish(vi1)

      vi2.lifecycleStage must be (LifecycleStage.Published)
      evaluating { tables.dropCopy(vi2) } must produce [IllegalArgumentException]
    }
  }

  test("Cannot drop the initial version when it's still unpublished") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn)
      val vi = tables.create("hello", "world")
      vi.lifecycleStage must be (LifecycleStage.Unpublished)
      evaluating { tables.dropCopy(vi) } must produce [IllegalArgumentException]
    }
  }

  test("Can drop a non-initial unpublished version") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn)
      val vi1 = tables.create("hello", "world")
      val vi2 = tables.publish(vi1)
      val vi3 = tables.ensureUnpublishedCopy(vi2.datasetInfo)
      tables.unpublished(vi1.datasetInfo) must equal (Some(vi3))

      tables.dropCopy(vi3)

      tables.unpublished(vi1.datasetInfo) must be (None)
    }
  }

  test("Can delete a table entirely") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn)
      val vi1 = tables.create("hello", "world")
      tables.addColumn(vi1, "col1", "typ1", "pcol1")
      tables.addColumn(vi1, "col2", "typ2", "pcol2")

      (1 to 5).foldLeft(vi1) { (vi, _) =>
        val vi2 = tables.publish(vi)
        tables.ensureUnpublishedCopy(vi2.datasetInfo)
      }

      // ok, there should be six copies now, which means twelve columns....
      count(conn, "column_map") must equal (12)
      count(conn, "version_map") must equal (6)
      count(conn, "dataset_map") must equal (1)

      tables.delete(vi1.datasetInfo)

      count(conn, "column_map") must equal (0)
      count(conn, "version_map") must equal (0)
      count(conn, "dataset_map") must equal (0)
    }
  }
}
