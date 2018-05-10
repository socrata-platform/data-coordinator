package com.socrata.datacoordinator.secondary.sql

import java.sql.Connection

import com.rojoma.simplearm.util.using
import com.socrata.datacoordinator.secondary.CollocationManifest

import scala.collection.mutable

abstract class SqlCollocationManifest(conn: Connection) extends CollocationManifest {
  override def collocatedDatasets(datasets: Set[String]): Set[String] = {
    def getNeighbors(dataset: String): Set[String] = {
      using(conn.prepareStatement(
        """SELECT dataset_internal_name_left FROM collocation_manifest WHERE dataset_internal_name_right = ?
          |UNION
          |SELECT dataset_internal_name_right FROM collocation_manifest WHERE dataset_internal_name_left = ?""".stripMargin))
      { stmt =>
        stmt.setString(1, dataset)
        stmt.setString(2, dataset)
        using(stmt.executeQuery()) { rs =>
          val result = Set.newBuilder[String]
          while (rs.next()) {
            result += rs.getString("dataset_internal_name_left")
          }
          result.result()
        }
      }
    }

    val toVisit = mutable.Queue.empty[String]
    val visited = mutable.Set.empty[String]

    datasets.foreach(toVisit.enqueue(_))

    while (toVisit.nonEmpty) {
      val current = toVisit.dequeue()
      visited.add(current)

      getNeighbors(current).foreach { neighbor =>
        if (!visited.contains(neighbor) && !toVisit.contains(neighbor)) toVisit.enqueue(neighbor)
      }
    }

    visited.toSet
  }

  override def dropCollocations(dataset: String): Unit = {
    using(conn.prepareStatement(
      """DELETE FROM collocation_manifest
        | WHERE dataset_internal_name_left = ?
        |    OR dataset_internal_name_right = ?
      """.stripMargin)) { stmt =>
      stmt.setString(1, dataset)
      stmt.setString(2, dataset)
      stmt.execute()
    }
  }
}
