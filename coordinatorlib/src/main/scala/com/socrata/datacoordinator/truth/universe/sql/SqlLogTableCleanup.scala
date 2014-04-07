package com.socrata.datacoordinator.truth.universe.sql

import java.sql.Connection

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.universe.LogTableCleanup
import scala.concurrent.duration.FiniteDuration

class SqlLogTableCleanup(conn: Connection, deleteOlderThan: FiniteDuration, deleteEvery: FiniteDuration) extends LogTableCleanup {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[SqlLogTableCleanup])

  def cleanupOldVersions(): Boolean = {
    using(conn.createStatement()) {
      stmt =>
        val datasetSystemId = using(stmt.executeQuery( s"""
            |SELECT system_id
            |FROM dataset_map
            |WHERE latest_data_version <> log_last_cleaned_data_version AND log_last_cleaned < NOW() - ('${deleteEvery.toSeconds} second' :: INTERVAL)
            |ORDER BY log_last_cleaned
            |LIMIT 1 FOR UPDATE
            """.stripMargin)) {
          rs =>
            if (rs.next()) {
              Some(rs.getLong("system_id"))
            } else {
              None
            }
        }

        datasetSystemId match {
          case None => false
          case Some(dataset_system_id) => {
            val auditTableName = s"t${dataset_system_id}_audit"
            val logTableName = s"t${dataset_system_id}_log"

            val deletableVersion = using(stmt.executeQuery( s"""
                |SELECT MAX(version) AS deletable_version
                |FROM ${auditTableName}
                |WHERE at_time < NOW() - ('${deleteOlderThan.toSeconds} second' :: INTERVAL)
                """.stripMargin)) {
              rs =>
                if (rs.next()) {
                  rs.getLong("deletable_version") match {
                    case 0 => None // getLong returns 0 if NULL
                    case n => Some(n)
                  }
                } else {
                  None
                }
            }

            deletableVersion.foreach {
              version =>
                val rowsDeleted = stmt.executeUpdate( s"""
                  |DELETE FROM ${logTableName}
                  |WHERE version <= ${version}
                  """.stripMargin)
                log.info(s"Removed ${rowsDeleted} rows up to version ${version} from ${logTableName}")

                stmt.executeUpdate(s"""
                  |UPDATE dataset_map
                  |SET log_last_cleaned_data_version = ${version},
                  |    log_last_cleaned = NOW()
                  |WHERE system_id = ${dataset_system_id}
                """.stripMargin)
            }
            true
          }
        }
    }
  }
}
