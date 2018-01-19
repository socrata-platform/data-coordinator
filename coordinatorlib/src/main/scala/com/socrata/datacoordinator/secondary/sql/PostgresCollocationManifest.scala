package com.socrata.datacoordinator.secondary.sql

import java.sql.{Connection, SQLException}

import com.rojoma.simplearm.util.using

class PostgresCollocationManifest(conn: Connection) extends SqlCollocationManifest(conn) {

  private val log = org.slf4j.LoggerFactory.getLogger(classOf[PostgresCollocationManifest])

  private val uniqueViolation = "23505"

  override def addCollocations(collocations: Set[(String, String)]): Unit = {
    val defaultAutoCommit = conn.getAutoCommit
    try {
      // I think this is not needed because auto commit is already false... but
      // set auto commit to false for doing batch inserts
      conn.setAutoCommit(false)

      using(conn.prepareStatement("SHOW server_version_num")) { showVersion =>
        using(showVersion.executeQuery()) { version =>
          if (version.next() && version.getInt("server_version_num") < 90500) { // I know I am a terrible human...
            // TODO: delete this case when we no longer have Postgres 9.4 servers running
            collocations.foreach { case (left, right) =>
              val savepoint = conn.setSavepoint()
              try {
                using(conn.prepareStatement(
                  """INSERT INTO collocation_manifest (dataset_internal_name_left, dataset_internal_name_right)
                    |     VALUES (? , ?)""".stripMargin)) { insert =>
                  insert.setString(1, left)
                  insert.setString(2, right)
                  insert.execute()
                }
              } catch {
                case e: java.sql.SQLException if e.getSQLState == uniqueViolation =>
                  conn.rollback(savepoint)
              } finally {
                try {
                  conn.releaseSavepoint(savepoint)
                } catch {
                  case e: SQLException =>
                    log.warn("Unexpected exception while releasing savepoint", e)
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
