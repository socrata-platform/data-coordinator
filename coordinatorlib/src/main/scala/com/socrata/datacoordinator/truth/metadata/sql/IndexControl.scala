package com.socrata.datacoordinator.truth.metadata.sql

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.rojoma.simplearm.v2.using

trait IndexControl[CT] { this: BasePostgresDatasetMapWriter[CT] =>

  private val upsertSql = """
      INSERT INTO index_directive_map(copy_system_id, column_system_id, directive)
             VALUES (?, ?, ?)
          ON CONFLICT(copy_system_id, column_system_id)
          DO UPDATE SET directive = ?,
                        updated_at = now(),
                        deleted_at = null
    """

  private val deleteSql = """
      UPDATE index_directive_map SET deleted_at = now()
       WHERE copy_system_id = ?
         AND column_system_id = ?
    """

  def createOrUpdateIndexDirective(columnInfo: ColumnInfo[CT], directive: JObject): Unit = {
    val directiveJson = JsonUtil.renderJson(directive)
    using(conn.prepareStatement(upsertSql)) { stmt =>
      stmt.setLong(1, columnInfo.copyInfo.systemId.underlying)
      stmt.setLong(2, columnInfo.systemId.underlying)
      stmt.setString(3, directiveJson)
      stmt.setString(4, directiveJson)
      stmt.execute()
    }
  }

  def dropIndexDirective(columnInfo: ColumnInfo[CT]): Unit = {
    using(conn.prepareStatement(deleteSql)) { stmt =>
      stmt.setLong(1, columnInfo.copyInfo.systemId.underlying)
      stmt.setLong(2, columnInfo.systemId.underlying)
      stmt.execute()
    }

  }
}
