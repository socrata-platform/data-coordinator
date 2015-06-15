package com.socrata.datacoordinator.secondary
package sql

import java.sql.{Connection, ResultSet, Timestamp}

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.util.TimingReport
import org.joda.time.DateTime

class SqlSecondaryConfig(conn: Connection, timingReport: TimingReport) extends SecondaryConfig {
  private def t = timingReport

  def list: Set[SecondaryConfigInfo] = {
    @annotation.tailrec
    def accumulate(acc: Set[SecondaryConfigInfo], rs: ResultSet): Set[SecondaryConfigInfo] =
      if (rs.next()) accumulate(acc + SecondaryConfigInfo(rs), rs) else acc

    val sql =
      """SELECT store_id, next_run_time, interval_in_seconds
        |  FROM secondary_stores_config
      """.stripMargin

    for {
      stmt <- managed(conn.prepareStatement(sql))
      rs <- managed(stmt.executeQuery())
    } yield accumulate(Set.empty, rs)
  }

  def lookup(storeId: String): Option[SecondaryConfigInfo] = {
    val sql = """
      SELECT store_id, next_run_time, interval_in_seconds
        FROM secondary_stores_config
       WHERE store_id = ?""".stripMargin

    for {
      stmt <- managed(conn.prepareStatement(sql))
      _ <- unmanaged(stmt.setString(1, storeId))
      rs <- managed(stmt.executeQuery())
    } yield if (rs.next()) Some(SecondaryConfigInfo(rs)) else None
  }

  def create(secondaryInfo: SecondaryConfigInfo): SecondaryConfigInfo =
    using(conn.prepareStatement(
      """INSERT INTO secondary_stores_config (store_id
        |  ,next_run_time
        |  ,interval_in_seconds
        |) VALUES (?, ?, ?)""".stripMargin)) { stmt =>
      stmt.setString(1, secondaryInfo.storeId)
      stmt.setTimestamp(2, new Timestamp(secondaryInfo.nextRunTime.getMillis))
      stmt.setInt(3, secondaryInfo.runIntervalSeconds)
      stmt.execute()
      secondaryInfo
    }

  def updateNextRunTime(storeId: String, newNextRunTime: DateTime) {
    using(conn.prepareStatement(
      """UPDATE secondary_stores_config
        |SET next_run_time = ?
        |WHERE store_id = ?""".stripMargin)) { stmt =>
      stmt.setTimestamp(1, new Timestamp(newNextRunTime.getMillis))
      stmt.setString(2, storeId)
      t("update-next-runtime", "store-id" -> storeId)(stmt.execute())
    }
  }
}
