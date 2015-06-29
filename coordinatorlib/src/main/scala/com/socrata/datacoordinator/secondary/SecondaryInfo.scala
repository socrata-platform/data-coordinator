package com.socrata.datacoordinator.secondary

import com.socrata.datacoordinator.secondary.sql.{SecondaryGroupInfo, SecondaryInstanceInfo}
import org.joda.time.DateTime

trait SecondaryInfo {
  def groups: Set[SecondaryGroupInfo]
  def instances: Set[SecondaryInstanceInfo]
  def instance(storeId: String): Option[SecondaryInstanceInfo]
  def create(secondaryInfo: SecondaryInstanceInfo): SecondaryInstanceInfo
  def updateNextRunTime(storeId: String, newNextRunTime: DateTime)
}
