package com.socrata.datacoordinator.secondary.sql

import java.sql.Connection

import com.rojoma.simplearm.util.using
import com.socrata.datacoordinator.secondary.CollocationManifest

class SqlCollocationManifest(conn: Connection) extends CollocationManifest {
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

  override def addCollocations(collocations: Set[(String, String)]): Unit = {
    val defaultAutoCommit = conn.getAutoCommit
    try {
      // I think this is not needed because auto commit is already false... but
      // set auto commit to false for doing batch inserts
      conn.setAutoCommit(false)

      using(conn.prepareStatement("SHOW server_version_num")) { showVersion =>
        using(showVersion.executeQuery()) { version =>
          if (version.getInt("server_version_num") < 90500) { // I know I am a terrible human...
            // TODO: delete this case when we no longer have Postgres 9.4 servers running
            collocations.foreach { case (left, right) =>
              using(conn.prepareStatement(
                """SELECT count(*) FROM collocation_manifest
                  | WHERE dataset_internal_name_left = ?
                  |   AND dataset_internal_name_right = ?""".stripMargin)) { selectCount =>
                selectCount.setString(1, left)
                selectCount.setString(2, right)

                // only insert a new entry since we have a unique constraint on the table
                using(selectCount.executeQuery()) { count =>
                  if (count.getInt("count") == 0) {
                    using(conn.prepareStatement(
                      """INSERT INTO collocation_manifest (dataset_internal_name_left, dataset_internal_name_right)
                        |     VALUES (? , ?)""".stripMargin)) { insert =>
                      insert.setString(1, left)
                      insert.setString(2, right)
                      insert.execute()
                    }
                  }
                }
              }
            }
          } else {
            // server is running at least Postgres 9.5 and ON CONFLICT is supported
            using(conn.prepareStatement(
              """INSERT INTO collocation_manifest (dataset_internal_name_left, dataset_internal_name_right)
                |     VALUES (? , ?)
                |ON CONFLICT DO NOTHING""".stripMargin)) { insert =>
              collocations.foreach { case (left, right) =>
                insert.setString(1, left)
                insert.setString(2, right)
                insert.addBatch()
              }

              insert.executeBatch()
            }
          }
        }
      }
    } finally {
      conn.setAutoCommit(defaultAutoCommit)
    }
  }
}
