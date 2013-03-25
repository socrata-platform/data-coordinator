package com.socrata.datacoordinator.truth.metadata.sql

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.matchers.MustMatchers
import java.sql.{SQLException, Connection, DriverManager}
import com.socrata.datacoordinator.truth.sql.{DatasetMapLimits, DatabasePopulator}
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.metadata.{CopyPair, ColumnAlreadyExistsException, LifecycleStage}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.id.numeric.{FixedSizeIdProvider, InMemoryBlockIdProvider, IdProvider}
import com.socrata.datacoordinator.id.ColumnId
import com.socrata.datacoordinator.util.NoopTimingReport
import scala.concurrent.duration.Duration

class PostgresDatasetMapWriterTest extends FunSuite with MustMatchers with BeforeAndAfterAll {
  def populateDatabase(conn: Connection) {
    val sql = DatabasePopulator.metadataTablesCreate(DatasetMapLimits())
    using(conn.createStatement()) { stmt =>
      stmt.execute(sql)
    }
  }

  def withDb[T]()(f: (Connection) => T): T = {
    using(DriverManager.getConnection("jdbc:postgresql://localhost:5432/blist_test", "blist", "blist")) { conn =>
      conn.setAutoCommit(false)
      populateDatabase(conn)
      f(conn)
    }
  }

  def idProvider = new FixedSizeIdProvider(new InMemoryBlockIdProvider(releasable = false), 1024)

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
      val tables = new PostgresDatasetMapWriter(conn, NoopTimingReport)
      val vi = tables.create("hello", "world")

      vi.datasetInfo.datasetName must be ("hello")
      vi.datasetInfo.tableBaseBase must be ("world")
      vi.lifecycleStage must be (LifecycleStage.Unpublished)
      vi.copyNumber must be (1)

      tables.datasetInfo(tables.datasetId("hello").get, Duration.Inf) must equal (Some(vi.datasetInfo))
      tables.unpublished(vi.datasetInfo) must equal (Some(vi))
    }
  }

  test("Can add a column to a table") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, NoopTimingReport)
      val vi = tables.create("hello", "world")
      val ci = tables.addColumn(vi, "col1", "typ", "colbase")

      ci.copyInfo must equal (vi)
      ci.logicalName must be ("col1")
      ci.typeName must be ("typ")
      ci.physicalColumnBaseBase must be ("colbase")
      ci.isUserPrimaryKey must be (false)

      tables.schema(vi) must equal (ColumnIdMap(ci.systemId -> ci))
    }
  }

  test("Can make a column a primary key") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, NoopTimingReport)
      val vi = tables.create("hello", "world")
      val ci = tables.addColumn(vi, "col1", "typ", "colbase")

      tables.setUserPrimaryKey(ci)

      tables.schema(vi) must equal (ColumnIdMap(ci.systemId -> ci.copy(isUserPrimaryKey = true)(null)))
    }
  }

  test("Can add a second column to a table") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, NoopTimingReport)
      val vi = tables.create("hello", "world")
      val ci1 = tables.addColumn(vi, "col1", "typ", "colbase")
      val ci2 = tables.addColumn(vi, "col2", "typ2", "colbase2")

      ci2.copyInfo must equal (vi)
      ci2.logicalName must be ("col2")
      ci2.typeName must be ("typ2")
      ci2.physicalColumnBaseBase must be ("colbase2")
      ci2.isUserPrimaryKey must be (false)

      tables.schema(vi) must equal (ColumnIdMap(ci1.systemId -> ci1, ci2.systemId -> ci2))
    }
  }

  test("Cannot have multiple primary keys") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, NoopTimingReport)
      val vi = tables.create("hello", "world")
      val ci1 = tables.addColumn(vi, "col1", "typ", "colbase")
      val ci2 = tables.addColumn(vi, "col2", "typ2", "colbase2")

      tables.setUserPrimaryKey(ci1)

      evaluating(tables.setUserPrimaryKey(ci2)) must produce [SQLException]
    }
  }


  test("Can clear a user primary key and re-seat it") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, NoopTimingReport)
      val vi = tables.create("hello", "world")
      val ci1 = tables.addColumn(vi, "col1", "typ", "colbase")
      val ci2 = tables.addColumn(vi, "col2", "typ2", "colbase2")

      val pk = tables.setUserPrimaryKey(ci1)
      tables.clearUserPrimaryKey(pk)
      tables.setUserPrimaryKey(ci2)

      tables.schema(vi) must equal (ColumnIdMap(ci1.systemId -> ci1, ci2.systemId -> ci2.copy(isUserPrimaryKey = true)(null)))
    }
  }

  test("Cannot add the same column twice") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, NoopTimingReport)
      val vi = tables.create("hello", "world")
      tables.addColumn(vi, "col1", "typ", "colbase")

      evaluating(tables.addColumn(vi, "col1", "typ2", "colbase2")) must produce [ColumnAlreadyExistsException]
    }
  }

  test("Can publish the initial working copy") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, NoopTimingReport)
      val vi1 = tables.create("hello", "world")
      val vi2 = tables.publish(vi1)
      vi2 must equal (vi1.copy(lifecycleStage = LifecycleStage.Published)(null))

      tables.published(vi2.datasetInfo) must equal (Some(vi2))
      tables.unpublished(vi1.datasetInfo) must be (None)
    }
  }

  test("Can drop a column") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, NoopTimingReport)
      val vi = tables.create("hello", "world")
      val c1 = tables.addColumn(vi, "col1", "typ1", "pcol1")
      val c2 = tables.addColumn(vi, "col2", "typ2", "pcol2")

      tables.dropColumn(c2)

      tables.schema(vi) must equal (ColumnIdMap(c1.systemId -> c1))
    }
  }

  test("Can make a working copy") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, NoopTimingReport)
      val vi1 = tables.publish(tables.create("hello", "world"))
      val ci1 = tables.addColumn(vi1, "col1", "typ", "colbase")
      val ci2 = tables.addColumn(vi1, "col2", "typ2", "colbase2")

      tables.unpublished(vi1.datasetInfo) must be (None)

      val Right(CopyPair(vi1a, vi2)) = tables.ensureUnpublishedCopy(vi1.datasetInfo)
      vi1a must equal (vi1)

      val schema1 = tables.schema(vi1)
      val schema2 = tables.schema(vi2)
      schema1.values.toSeq.map(_.unanchored).sortBy(_.logicalName) must equal (schema2.values.toSeq.map(_.unanchored).sortBy(_.logicalName))
    }
  }

  test("Cannot drop a published version") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, NoopTimingReport)
      val vi1 = tables.create("hello", "world")
      val vi2 = tables.publish(vi1)

      vi2.lifecycleStage must be (LifecycleStage.Published)
      evaluating { tables.dropCopy(vi2) } must produce [IllegalArgumentException]
    }
  }

  test("Cannot drop the initial version when it's still unpublished") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, NoopTimingReport)
      val vi = tables.create("hello", "world")
      vi.lifecycleStage must be (LifecycleStage.Unpublished)
      evaluating { tables.dropCopy(vi) } must produce [IllegalArgumentException]
    }
  }

  test("Can drop a non-initial unpublished version") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, NoopTimingReport)
      val vi1 = tables.create("hello", "world")
      val vi2 = tables.publish(vi1)
      val Right(CopyPair(_, vi3)) = tables.ensureUnpublishedCopy(vi2.datasetInfo)
      tables.unpublished(vi1.datasetInfo) must equal (Some(vi3))

      tables.dropCopy(vi3)

      tables.unpublished(vi1.datasetInfo) must be (None)
    }
  }

  test("Column IDs are allocated into the first gap if one exists") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, NoopTimingReport)
      val vi = tables.create("hello", "world")
      tables.addColumnWithId(new ColumnId(0), vi, "col0", "typ0", "t")
      tables.addColumnWithId(new ColumnId(2), vi, "col2", "typ2", "t")
      tables.addColumn(vi, "col1", "typ1", "t").systemId must equal (new ColumnId(1))
    }
  }

  test("Column IDs are allocated as 0 if it doesn't exist") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, NoopTimingReport)
      val vi = tables.create("hello", "world")
      tables.addColumnWithId(new ColumnId(1), vi, "col1", "typ1", "t")
      tables.addColumnWithId(new ColumnId(2), vi, "col2", "typ2", "t")
      tables.addColumn(vi, "col0", "typ0", "t").systemId must equal (new ColumnId(0))
    }
  }

  test("Column IDs are allocated at the end if there are no gaps") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, NoopTimingReport)
      val vi = tables.create("hello", "world")
      tables.addColumnWithId(new ColumnId(0), vi, "col0", "typ0", "t")
      tables.addColumnWithId(new ColumnId(1), vi, "col1", "typ1", "t")
      tables.addColumn(vi, "col2", "typ2", "t").systemId must equal (new ColumnId(2))
    }
  }

  test("Adding a column to a table does not use IDs from this table or the previous version") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, NoopTimingReport)
      val vi0 = tables.create("hello", "world")
      val ci0 = tables.addColumn(vi0, "col0", "typ0", "base0")
      val ci1 = tables.addColumn(vi0, "col1", "typ1", "base1")
      val vi1 = tables.ensureUnpublishedCopy(tables.publish(vi0).datasetInfo).right.get.newCopyInfo
      val ci2 = tables.addColumn(vi1, "col2", "typ2", "base2")
      tables.dropColumn(tables.schema(vi1)(new ColumnId(1)))
      val ci3 = tables.addColumn(vi1, "col3", "typ3", "base3")

      ci3.systemId must be (new ColumnId(3))
    }
  }

  test("Can delete a table entirely") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, NoopTimingReport)
      val vi1 = tables.create("hello", "world")
      tables.addColumn(vi1, "col1", "typ1", "pcol1")
      tables.addColumn(vi1, "col2", "typ2", "pcol2")

      (1 to 5).foldLeft(vi1) { (vi, _) =>
        val vi2 = tables.publish(vi)
        tables.ensureUnpublishedCopy(vi2.datasetInfo).right.get.newCopyInfo
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
}
