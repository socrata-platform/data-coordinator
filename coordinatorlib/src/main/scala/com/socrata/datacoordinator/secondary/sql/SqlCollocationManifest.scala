package com.socrata.datacoordinator.secondary.sql

import java.sql.Connection
import java.util.UUID

import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.secondary.CollocationManifest

import scala.collection.mutable

abstract class SqlCollocationManifest(conn: Connection) extends CollocationManifest {
  override def collocatedDatasets(datasets: Set[String]): Set[String] = {
    val sql =
      """WITH RECURSIVE collocations (dataset_internal_name) AS (
        |  SELECT unnest(?)
        |UNION
        |  SELECT
        |    unnest(array[cm.dataset_internal_name_left, cm.dataset_internal_name_right])
        |  FROM
        |    collocations
        |    JOIN collocation_manifest cm
        |      ON cm.dataset_internal_name_right = collocations.dataset_internal_name
        |      OR cm.dataset_internal_name_left = collocations.dataset_internal_name
        |)
        |SELECT dataset_internal_name FROM collocations""".stripMargin
    for {
      stmt <- managed(conn.prepareStatement(sql))
        .and(_.setObject(1, datasets.toArray)) // using an array like this is a postgresqlism
      rs <- managed(stmt.executeQuery())
    } {
      val result = Set.newBuilder[String]
      while(rs.next()) {
        result += rs.getString("dataset_internal_name")
      }
      result.result()
    }
  }

  override def dropCollocations(dataset: String): Unit = {
    using(conn.prepareStatement(
      """UPDATE collocation_manifest SET deleted_at = now()
        | WHERE dataset_internal_name_left = ?
        |    OR dataset_internal_name_right = ?
      """.stripMargin)) { stmt =>
      stmt.setString(1, dataset)
      stmt.setString(2, dataset)
      stmt.execute()
    }
  }

  override def dropCollocations(jobId: UUID): Unit = {
    using(conn.prepareStatement("UPDATE collocation_manifest SET deleted_at = now() WHERE job_id = ?")) { stmt =>
      stmt.setObject(1, jobId)
      stmt.execute()
    }
  }
}
