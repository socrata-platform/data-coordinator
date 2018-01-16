package com.socrata.datacoordinator.secondary.sql

import java.sql.Connection

import com.rojoma.simplearm.util.using
import com.socrata.datacoordinator.secondary.CollocationManifest

abstract class SqlCollocationManifest(conn: Connection) extends CollocationManifest {
  override def collocatedDatasets(datasets: Set[String]) = {
    using(conn.prepareStatement(
      """WITH RECURSIVE related_data_sets(dataset_internal_name_left, dataset_internal_name_right, system_id, path, cycle) AS (
        |    SELECT dataset_internal_name_left, dataset_internal_name_right, system_id, ARRAY[system_id], false
        |      FROM collocation_manifest WHERE dataset_internal_name_left = ANY(?) OR dataset_internal_name_right = ANY(?)
        |    UNION ALL
        |    SELECT src.dataset_internal_name_left, src.dataset_internal_name_right, src.system_id, path || src.system_id, src.system_id = ANY(path)
        |      FROM collocation_manifest src, related_data_sets rn
        |      WHERE (
        |          src.dataset_internal_name_left=rn.dataset_internal_name_right OR
        |          src.dataset_internal_name_right=rn.dataset_internal_name_left OR
        |          src.dataset_internal_name_left=rn.dataset_internal_name_left OR
        |          src.dataset_internal_name_right=rn.dataset_internal_name_right
        |        )
        |        AND NOT cycle
        |      )
        |SELECT dataset_internal_name_left FROM related_data_sets
        |UNION
        |SELECT dataset_internal_name_right FROM related_data_sets""".stripMargin))
    { stmt =>
      val datasetsArray =  conn.createArrayOf("varchar", datasets.toArray)
      stmt.setArray(1, datasetsArray)
      stmt.setArray(2, datasetsArray)
      using(stmt.executeQuery()) { rs =>
        val result = Set.newBuilder[String]
        while (rs.next()) {
          result += rs.getString("dataset_internal_name_left")
        }
        result.result()
      }
    }
  }
}
