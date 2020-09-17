package com.socrata.datacoordinator.truth.metadata.sql

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.rojoma.simplearm.v2.using

trait IndexControl[CT] { this: BasePostgresDatasetMapWriter[CT] =>

  private val upsertSql = """
      INSERT INTO index_directives(dataset_system_id, field_name_casefolded, directives)
             VALUES (?, ?, ?)
          ON CONFLICT(dataset_system_id, field_name_casefolded)
          DO UPDATE SET directives = ?,
                        updated_at = now(),
                        deleted_at = null
    """

  private val deleteSql = """
      UPDATE index_directives SET deleted_at = now()
       WHERE dataset_system_id = ?
         AND field_name_casefolded = ?
    """

  def createIndexDirectives(columnInfo: ColumnInfo[CT], directives: JObject): Unit = {
    columnInfo.fieldName.foreach { fieldName =>
      val directivesJson = JsonUtil.renderJson(directives)
      using(conn.prepareStatement(upsertSql)) { stmt =>
        stmt.setLong(1, columnInfo.copyInfo.datasetInfo.systemId.underlying)
        stmt.setString(2, fieldName.caseFolded)
        stmt.setString(3, directivesJson)
        stmt.setString(4, directivesJson)
        stmt.execute()
      }
    }
  }

  def deleteIndexDirectives(columnInfo: ColumnInfo[CT]): Unit = {
    columnInfo.fieldName.foreach { fieldName =>
      using(conn.prepareStatement(deleteSql)) { stmt =>
        stmt.setLong(1, columnInfo.copyInfo.datasetInfo.systemId.underlying)
        stmt.setString(2, fieldName.caseFolded)
        stmt.execute()
      }
    }
  }
}
