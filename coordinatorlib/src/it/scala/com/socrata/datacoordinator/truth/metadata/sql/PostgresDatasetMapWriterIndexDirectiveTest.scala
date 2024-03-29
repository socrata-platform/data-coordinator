package com.socrata.datacoordinator.truth.metadata.sql

import com.rojoma.json.v3.ast.{JBoolean, JObject}
import com.socrata.datacoordinator.truth.metadata.CopyPair
import com.socrata.datacoordinator.util.NoopTimingReport

trait PostgresDatasetMapWriterIndexDirectiveTest { this: PostgresDatasetMapWriterTest =>

  test("Can create, update and delete index directive and survive publication") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi1 = tables.create("en_US", resourcName)
      val ci1 = tables.addColumn(vi1, c("col1"), fn("field1"), t("typ1"), "pcol1")
      val directiveEnabled = JObject(Map("enabled" -> JBoolean.canonicalTrue))
      tables.createOrUpdateIndexDirective(ci1, directiveEnabled)

      val indexes  = tables.indexDirectives(vi1, None)
      indexes.size must equal (1)
      val index = indexes.head
      index.columnInfo must equal (ci1)
      index.directive must equal (directiveEnabled)

      val directiveDisabled = JObject(Map("enabled" -> JBoolean.canonicalFalse))
      tables.createOrUpdateIndexDirective(ci1, directiveDisabled)

      val indexesUpdated  = tables.indexDirectives(vi1, None)
      indexesUpdated.size must equal (1)
      val indexUpdated = indexesUpdated.head
      index.columnInfo must equal (index.columnInfo)
      indexUpdated.directive must equal (directiveDisabled)

      // run publication cycle and re-check index
      val (vi1p, _) =tables.publish(vi1)
      val Right(CopyPair(_, vi2u)) = tables.ensureUnpublishedCopy(vi1p.datasetInfo)
      val (vi2p, _) = tables.publish(vi2u)
      val c2i = tables.indexDirectives(vi2p, None).head
      c2i.copyInfo must equal (vi2p)
      c2i.directive must equal (indexUpdated.directive)

      tables.dropIndexDirective(c2i.columnInfo)
      tables.indexDirectives(vi2p, None) must equal (Seq.empty)
    }
  }

  test("Get directive of a specific column") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi1 = tables.create("en_US", resourcName)
      val ci1 = tables.addColumn(vi1, c("col1"), fn("field1"), t("typ1"), "pcol1")
      val ci2 = tables.addColumn(vi1, c("col2"), fn("field2"), t("typ1"), "pcol2")
      val directiveEnabled = JObject(Map("enabled" -> JBoolean.canonicalTrue))
      tables.createOrUpdateIndexDirective(ci1, directiveEnabled)
      tables.createOrUpdateIndexDirective(ci2, directiveEnabled)

      val allIndexes  = tables.indexDirectives(vi1, None)
      val ci2Index  = tables.indexDirectives(vi1, ci2.fieldName)
      allIndexes.size must equal (2)
      ci2Index.size must equal (1)
      val index = ci2Index.head
      index.columnInfo must equal (ci2)
      index.directive must equal (directiveEnabled)
    }
  }
}
