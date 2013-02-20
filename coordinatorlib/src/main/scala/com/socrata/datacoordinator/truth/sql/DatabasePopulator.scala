package com.socrata.datacoordinator.truth.sql

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.util.TemplateReplacer

object DatabasePopulator {
  private def load(template: String) =
    using(getClass.getResourceAsStream(template)) { stream =>
      scala.io.Source.fromInputStream(stream, "UTF-8").getLines().mkString("\n")
    }

  def metadataTablesCreate(datasetMapLimits: DatasetMapLimits): String = {
    import datasetMapLimits._
    TemplateReplacer(
      load("metadata.tmpl.sql"),
      Map(
        "dataset_id_len" -> maximumDatasetIdLength.toString,
        "user_uid_len" -> maximumUserIdLength.toString,
        "column_name_len" -> maximumColumnNameLength.toString,
        "physcol_base_len" -> maximumPhysicalColumnBaseLength.toString,
        "phystab_base_len" -> maximumPhysicalTableBaseLength.toString,
        "type_name_len" -> maximumTypeNameLength.toString,
        "store_id_len" -> maximumStoreIdLength.toString,
        "table_name_len" -> maximumPhysicalTableNameLength.toString
      )
    )
  }

  def logTableCreate(tableName: String,
                     operationLen: Int): String =
    TemplateReplacer(
      load("table_log.tmpl.sql"),
      Map(
        "table_name" -> tableName,
        "operation_len" -> operationLen.toString
      ))

  def populate(conn: java.sql.Connection,
               datasetMapLimits: DatasetMapLimits) {
    using(conn.createStatement()) { stmt => stmt.execute(metadataTablesCreate(datasetMapLimits)) }
  }
}

case class DatasetMapLimits(maximumDatasetIdLength: Int = 20,
                            maximumUserIdLength: Int = 20,
                            maximumColumnNameLength: Int = 20,
                            maximumPhysicalColumnBaseLength: Int = 20,
                            maximumPhysicalTableBaseLength: Int = 20,
                            maximumTypeNameLength: Int = 20,
                            maximumStoreIdLength: Int = 20,
                            maximumPhysicalTableNameLength: Int = 60)
