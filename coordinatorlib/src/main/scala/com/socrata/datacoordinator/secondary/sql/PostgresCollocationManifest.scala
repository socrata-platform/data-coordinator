package com.socrata.datacoordinator.secondary.sql

import java.sql.{Connection, SQLException}
import java.util.UUID

import com.rojoma.simplearm.util.using

class PostgresCollocationManifest(conn: Connection) extends SqlCollocationManifest(conn) {

  private val log = org.slf4j.LoggerFactory.getLogger(classOf[PostgresCollocationManifest])

  private val uniqueViolation = "23505"

  override def addCollocations(jobId: UUID, collocations: Set[(String, String)]): Unit = {
    val defaultAutoCommit = conn.getAutoCommit
    try {
      // I think this is not needed because auto commit is already false... but
      // set auto commit to false for doing batch inserts
      conn.setAutoCommit(false)
        // server is running at least Postgres 9.5 and ON CONFLICT is supported
      using(conn.prepareStatement(
        """INSERT INTO collocation_manifest (job_id, dataset_internal_name_left, dataset_internal_name_right)
          |     VALUES (?, ? , ?)
          |ON CONFLICT DO NOTHING""".stripMargin)) { insert =>
        collocations.foreach { case (left, right) =>
          insert.setObject(1, jobId)
          insert.setString(2, left)
          insert.setString(3, right)
          insert.addBatch()
        }

        insert.executeBatch()
      }
    }
  }
}
