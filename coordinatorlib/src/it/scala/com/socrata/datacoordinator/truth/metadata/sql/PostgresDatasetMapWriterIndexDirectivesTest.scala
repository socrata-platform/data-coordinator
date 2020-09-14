package com.socrata.datacoordinator.truth.metadata.sql

import com.rojoma.json.v3.ast.{JObject, JBoolean, JValue}
import com.socrata.datacoordinator.util.NoopTimingReport

trait PostgresDatasetMapWriterIndexDirectivesTest { this: PostgresDatasetMapWriterTest =>

  test("Can create, update and delete index directives") {
    withDb() { conn =>
      val tables = new PostgresDatasetMapWriter(conn, noopTypeNamespace, NoopTimingReport, noopKeyGen, ZeroID, ZeroVersion)
      val vi1 = tables.create("en_US", resourcName)
      val fieldName = fn("col")
      val ci1 = tables.addColumn(vi1, c("col1"), fieldName, t("typ1"), "pcol1")
      val directivesEnabled = JObject(Map("enabled" -> JBoolean.canonicalTrue))
      tables.createIndexDirectives(ci1, directivesEnabled)

      val indexes  = tables.indexDirectives(vi1.datasetInfo)
      indexes.size must equal (1)
      val index = indexes.head
      index.fieldName must equal (fieldName.get)
      index.directives must equal (directivesEnabled)

      val directivesDisabled = JObject(Map("enabled" -> JBoolean.canonicalFalse))
      tables.createIndexDirectives(ci1, directivesDisabled)

      val indexesUpdated  = tables.indexDirectives(vi1.datasetInfo)
      indexesUpdated.size must equal (1)
      val indexUpdated = indexesUpdated.head
      indexUpdated.fieldName must equal (fieldName.get)
      indexUpdated.directives must equal (directivesDisabled)

      tables.deleteIndexDirectives(ci1)
      tables.indexDirectives(vi1.datasetInfo) must equal (Seq.empty)
    }
  }

}
