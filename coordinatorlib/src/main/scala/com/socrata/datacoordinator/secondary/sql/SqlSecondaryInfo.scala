package com.socrata.datacoordinator.secondary
package sql

import java.sql.{Connection, ResultSet, Timestamp}

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.util.TimingReport
import org.joda.time.DateTime

class SqlSecondaryInfo(conn: Connection, timingReport: TimingReport) extends SecondaryInfo {
  private def t = timingReport

  def groups: Set[SecondaryGroupInfo] = {
    @annotation.tailrec
    def accumulate(acc: Set[SecondaryGroupInfo], rs: ResultSet): Set[SecondaryGroupInfo] =
      if (rs.next()) accumulate(acc + SecondaryGroupInfo(rs), rs) else acc

    val sql =
      """SELECT group_name, is_default, instances, num_replicas
        |  FROM secondary_groups_config
      """.stripMargin

    for {
      stmt <- managed(conn.prepareStatement(sql))
      rs <- managed(stmt.executeQuery())
    } yield accumulate(Set.empty, rs)
  }

  def instances: Set[SecondaryInstanceInfo] = {
    @annotation.tailrec
    def accumulate(acc: Set[SecondaryInstanceInfo], rs: ResultSet): Set[SecondaryInstanceInfo] =
      if (rs.next()) accumulate(acc + SecondaryInstanceInfo(rs), rs) else acc

    val sql =
      """SELECT store_id, next_run_time, interval_in_seconds
        |  FROM secondary_stores_config
      """.stripMargin

    for {
      stmt <- managed(conn.prepareStatement(sql))
      rs <- managed(stmt.executeQuery())
    } yield accumulate(Set.empty, rs)
  }

  def instance(storeId: String): Option[SecondaryInstanceInfo] = {
    val sql =
      """SELECT store_id, next_run_time, interval_in_seconds
        |  FROM secondary_stores_config
        | WHERE store_id = ?
      """.stripMargin

    for {
      stmt <- managed(conn.prepareStatement(sql))
      _ <- unmanaged(stmt.setString(1, storeId))
      rs <- managed(stmt.executeQuery())
    } yield if (rs.next()) Some(SecondaryInstanceInfo(rs)) else None
  }

  def create(secondaryInfo: SecondaryInstanceInfo): SecondaryInstanceInfo =
    using(conn.prepareStatement(
      """INSERT INTO secondary_stores_config (store_id
        |  ,next_run_time
        |  ,interval_in_seconds
        |) VALUES (?, ?, ?)
      """.stripMargin)) { stmt =>
      stmt.setString(1, secondaryInfo.storeId)
      stmt.setTimestamp(2, new Timestamp(secondaryInfo.nextRunTime.getMillis))
      stmt.setInt(3, secondaryInfo.runIntervalSeconds)
      stmt.execute()
      secondaryInfo
    }

  def updateNextRunTime(storeId: String, newNextRunTime: DateTime) {
    using(conn.prepareStatement(
      """UPDATE secondary_stores_config
        |   SET next_run_time = ?
        | WHERE store_id = ?
      """.stripMargin)) { stmt =>
      stmt.setTimestamp(1, new Timestamp(newNextRunTime.getMillis))
      stmt.setString(2, storeId)
      t("update-next-runtime", "store-id" -> storeId)(stmt.execute())
    }
  }
}

case class SecondaryInstanceInfo(storeId: String, nextRunTime: DateTime, runIntervalSeconds: Int)
object SecondaryInstanceInfo {
  def apply(rs: ResultSet): SecondaryInstanceInfo =
    SecondaryInstanceInfo(rs.getString("store_id"),
      new DateTime(rs.getTimestamp("next_run_time").getTime),
      rs.getInt("interval_in_seconds"))
}

case class SecondaryGroupInfo(groupId: String, isDefault: Boolean, instances: Set[String], numReplicas: Int)
object SecondaryGroupInfo {
  def apply(rs: ResultSet): SecondaryGroupInfo =
    SecondaryGroupInfo(rs.getString("group_name"),
      rs.getBoolean("is_default"),
      rs.getArray("instances").getArray.asInstanceOf[Array[String]].toSet,
      rs.getInt("num_replicas"))
}
