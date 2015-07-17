package com.socrata.datacoordinator.truth.sql

import java.sql.Connection

import scala.io.{Source, Codec}

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.util.TemplateReplacer

object DatabasePopulator {
  private def load(template: String) =
    using(getClass.getResourceAsStream(template)) { stream =>
      Source.fromInputStream(stream)(Codec.UTF8).getLines().mkString("\n")
    }

  def logTableCreate(auditTableName: String,
                     tableName: String,
                     userUidLen: Int,
                     operationLen: Int,
                     user: String,
                     tablespace: Option[String]): String =
    TemplateReplacer(
      load("table_log.tmpl.sql"),
      Map(
        "audit_table_name" -> auditTableName,
        "user_uid_len" -> userUidLen.toString,
        "table_name" -> tableName,
        "operation_len" -> operationLen.toString,
        "user" -> user, // table owner
        "tablespace" -> tablespace.fold("") { ts => "TABLESPACE " + ts }
      ))
}

case class DatasetMapLimits(maximumUserIdLength: Int = 40,
                            maximumLogicalColumnNameLength: Int = 40,
                            maximumPhysicalColumnBaseLength: Int = 40,
                            maximumTypeNameLength: Int = 40,
                            maximumStoreIdLength: Int = 40,
                            maximumLocaleNameLength: Int = 40,
                            maximumPhysicalTableNameLength: Int = 80)
