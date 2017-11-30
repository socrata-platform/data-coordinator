package com.socrata.datacoordinator.secondary.sql

import java.sql.Connection

import com.rojoma.simplearm.util.using
import com.socrata.datacoordinator.secondary.CollocationManifest

class SqlCollocationManifest(conn: Connection) extends CollocationManifest {
  override def collocatedDatasets(datasets: Set[String]) = {
    using(conn.prepareStatement(
      """WITH RECURSIVE related_data_sets(dataset_internal_name_a, dataset_internal_name_b, system_id, path, cycle) AS (
        |    SELECT dataset_internal_name_a, dataset_internal_name_b, system_id, ARRAY[system_id], false
        |      FROM collocation_manifest WHERE dataset_internal_name_a = ANY(?) OR dataset_internal_name_b = ANY(?)
        |    UNION ALL
        |    SELECT src.dataset_internal_name_a, src.dataset_internal_name_b, src.system_id, path || src.system_id, src.system_id = ANY(path)
        |      FROM collocation_manifest src, related_data_sets rn
        |      WHERE (
        |          src.dataset_internal_name_a=rn.dataset_internal_name_b OR
        |          src.dataset_internal_name_b=rn.dataset_internal_name_a OR
        |          src.dataset_internal_name_a=rn.dataset_internal_name_a OR
        |          src.dataset_internal_name_b=rn.dataset_internal_name_b
        |        )
        |        AND NOT cycle
        |      )
        |SELECT dataset_internal_name_a FROM related_data_sets
        |UNION
        |SELECT dataset_internal_name_b FROM related_data_sets""".stripMargin))
    { stmt =>
      val datasetsArray =  conn.createArrayOf("varchar", datasets.toArray)
      stmt.setArray(1, datasetsArray)
      stmt.setArray(2, datasetsArray)
      using(stmt.executeQuery()) { rs =>
        val result = Set.newBuilder[String]
        while (rs.next()) {
          result += rs.getString("dataset_internal_name_a")
        }
        result.result()
      }
    }
  }
}
