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
        "user_uid_len" -> maximumUserIdLength.toString,
        "logical_name_len" -> maximumLogicalColumnNameLength.toString,
        "physcol_base_len" -> maximumPhysicalColumnBaseLength.toString,
        "phystab_base_len" -> maximumPhysicalTableBaseLength.toString,
        "type_name_len" -> maximumTypeNameLength.toString,
        "store_id_len" -> maximumStoreIdLength.toString,
        "table_name_len" -> maximumPhysicalTableNameLength.toString,
        "locale_name_len" -> maximumLocaleNameLength.toString
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

case class DatasetMapLimits(maximumUserIdLength: Int = 40,
                            maximumLogicalColumnNameLength: Int = 40,
                            maximumPhysicalColumnBaseLength: Int = 40,
                            maximumPhysicalTableBaseLength: Int = 40,
                            maximumTypeNameLength: Int = 40,
                            maximumStoreIdLength: Int = 40,
                            maximumLocaleNameLength: Int = 40,
                            maximumPhysicalTableNameLength: Int = 80)
