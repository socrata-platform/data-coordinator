package com.socrata.datacoordinator.truth.metadata.sql

import com.rojoma.json.v3.ast.{JObject, JBoolean}
import com.socrata.datacoordinator.util.NoopTimingReport

trait PostgresDatasetMapWriterIndexDirectiveTest { this: PostgresDatasetMapWriterTest =>

  test("Can create, update and delete index directive") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi1 = tables.create("en_US", resourcName)
      val ci1 = tables.addColumn(vi1, c("col1"), fn("field1"), t("typ1"), "pcol1")
      val directiveEnabled = JObject(Map("enabled" -> JBoolean.canonicalTrue))
      tables.createOrUpdateIndexDirective(ci1, directiveEnabled)

      val indexes  = tables.indexDirectives(vi1)
      indexes.size must equal (1)
      val index = indexes.head
      index.columnInfo must equal (index.columnInfo)
      index.directive must equal (directiveEnabled)

      val directiveDisabled = JObject(Map("enabled" -> JBoolean.canonicalFalse))
      tables.createOrUpdateIndexDirective(ci1, directiveDisabled)

      val indexesUpdated  = tables.indexDirectives(vi1)
      indexesUpdated.size must equal (1)
      val indexUpdated = indexesUpdated.head
      index.columnInfo must equal (index.columnInfo)
      indexUpdated.directive must equal (directiveDisabled)

      tables.dropIndexDirective(ci1)
      tables.indexDirectives(vi1) must equal (Seq.empty)
    }
  }

}
