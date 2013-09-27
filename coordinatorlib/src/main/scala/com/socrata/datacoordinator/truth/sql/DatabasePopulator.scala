package com.socrata.datacoordinator.truth.sql

import scala.io.{Source, Codec}

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.util.TemplateReplacer

object DatabasePopulator {
  private def load(template: String) =
    using(getClass.getResourceAsStream(template)) { stream =>
      Source.fromInputStream(stream)(Codec.UTF8).getLines().mkString("\n")
    }

  def metadataTablesCreate(datasetMapLimits: DatasetMapLimits): String = {
    import datasetMapLimits._
    TemplateReplacer(
      load("metadata.tmpl.sql"),
      Map(
        "user_uid_len" -> maximumUserIdLength.toString,
        "user_column_id_len" -> maximumLogicalColumnNameLength.toString,
        "physcol_base_len" -> maximumPhysicalColumnBaseLength.toString,
        "type_name_len" -> maximumTypeNameLength.toString,
        "store_id_len" -> maximumStoreIdLength.toString,
        "table_name_len" -> maximumPhysicalTableNameLength.toString,
        "locale_name_len" -> maximumLocaleNameLength.toString
      )
    )
  }

  def logTableCreate(auditTableName: String,
                     tableName: String,
                     userUidLen: Int,
                     operationLen: Int,
                     tablespace: Option[String]): String =
    TemplateReplacer(
      load("table_log.tmpl.sql"),
      Map(
        "audit_table_name" -> auditTableName,
        "user_uid_len" -> userUidLen.toString,
        "table_name" -> tableName,
        "operation_len" -> operationLen.toString,
        "tablespace" -> tablespace.fold("") { ts => "TABLESPACE " + ts }
      ))

  def populate(conn: java.sql.Connection,
               datasetMapLimits: DatasetMapLimits) {
    using(conn.createStatement()) { stmt => stmt.execute(metadataTablesCreate(datasetMapLimits)) }
  }
}

case class DatasetMapLimits(maximumUserIdLength: Int = 40,
                            maximumLogicalColumnNameLength: Int = 40,
                            maximumPhysicalColumnBaseLength: Int = 40,
                            maximumTypeNameLength: Int = 40,
                            maximumStoreIdLength: Int = 40,
                            maximumLocaleNameLength: Int = 40,
                            maximumPhysicalTableNameLength: Int = 80)
