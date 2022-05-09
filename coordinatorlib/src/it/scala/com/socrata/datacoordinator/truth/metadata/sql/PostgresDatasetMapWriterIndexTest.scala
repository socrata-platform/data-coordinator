package com.socrata.datacoordinator.truth.metadata.sql

import com.socrata.datacoordinator.id.IndexName
import com.socrata.datacoordinator.truth.loader.sql.messages.LogData.UnanchoredIndexInfo
import com.socrata.datacoordinator.truth.metadata.CopyPair
import com.socrata.datacoordinator.util.NoopTimingReport

trait PostgresDatasetMapWriterIndexTest { this: PostgresDatasetMapWriterTest =>

  test("Can create, update, delete index and survive publication") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi1 = tables.create("en_US", resourcName)
      val i1 = UnanchoredIndexInfo(-1, "i1", "a,b || c", Some("is_deleted is null"))
      // create
      tables.createOrUpdateIndex(vi1, new IndexName(i1.name), i1.expressions, i1.filter)
      val indexes  = tables.indexes(vi1)
      indexes.size must equal (1)
      val index = indexes.head
      index.name.underlying must equal (i1.name)
      index.expressions must equal (i1.expressions)
      index.filter must equal (i1.filter)

      val indexUpdated = index.copy(expressions = "a,b", filter = None)
      // update
      tables.createOrUpdateIndex(vi1, indexUpdated.name, indexUpdated.expressions, indexUpdated.filter)
      tables.indexes(vi1).head must equal (indexUpdated)

      // run publication cycle and re-check index
      val (vi1p, _) =tables.publish(vi1)
      val Right(CopyPair(_, vi2u)) = tables.ensureUnpublishedCopy(vi1p.datasetInfo)
      val (vi2p, _) = tables.publish(vi2u)
      val c2i = tables.indexes(vi2p).head
      c2i.name must equal (indexUpdated.name)
      c2i.copyInfo must equal (vi2p)
      c2i.expressions must equal (indexUpdated.expressions)
      c2i.filter must equal (indexUpdated.filter)

      // delete
      tables.dropIndex(vi2p, None)
      tables.indexes(vi2p) must equal (Nil)
    }
  }
}
