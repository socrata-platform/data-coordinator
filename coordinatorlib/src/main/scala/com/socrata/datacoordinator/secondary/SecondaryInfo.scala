package com.socrata.datacoordinator.secondary

import java.sql.ResultSet

import org.joda.time.DateTime

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
    SecondaryGroupInfo(rs.getString("id"),
      rs.getBoolean("is_default"),
      rs.getString("instances").split("[ ,;]").toSet.map((s: String) => s.trim),
      rs.getInt("num_replicas"))
}

trait SecondaryInfo {
  def defaultGroups: Set[SecondaryGroupInfo] = groups.filter(_.isDefault)
  def groups: Set[SecondaryGroupInfo]
  def instances: Set[SecondaryInstanceInfo]
  def instance(storeId: String): Option[SecondaryInstanceInfo]
  def create(secondaryInfo: SecondaryInstanceInfo): SecondaryInstanceInfo
  def updateNextRunTime(storeId: String, newNextRunTime: DateTime)
}
