package com.socrata.datacoordinator.truth.sql

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.util.TemplateReplacer

object DatabasePopulator {
  private def load(template: String) =
    using(getClass.getResourceAsStream(template)) { stream =>
      scala.io.Source.fromInputStream(stream, "UTF-8").getLines().mkString("\n")
    }

  def metadataTablesCreate(datasetIdLen: Int,
                           userUidLen: Int,
                           columnNameLen: Int,
                           physcolBaseLen: Int,
                           phystabBaseLen: Int,
                           typeNameLen: Int,
                           storeIdLen: Int): String =
    TemplateReplacer(
      load("metadata.tmpl.sql"),
      Map(
        "dataset_id_len" -> datasetIdLen.toString,
        "user_uid_len" -> userUidLen.toString,
        "column_name_len" -> columnNameLen.toString,
        "physcol_base_len" -> physcolBaseLen.toString,
        "phystab_base_len" -> phystabBaseLen.toString,
        "type_name_len" -> typeNameLen.toString,
        "store_id_len" -> storeIdLen.toString
      )
    )

  def logTableCreate(tableName: String,
                     operationLen: Int): String =
    TemplateReplacer(
      load("table_log.tmpl.sql"),
      Map(
        "table_name" -> tableName,
        "operation_len" -> operationLen.toString
      ))
}
