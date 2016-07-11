package com.socrata.datacoordinator.secondary

import org.joda.time.DateTime

case class SecondaryConfigInfo(storeId: String, nextRunTime: DateTime, runIntervalSeconds: Int)

trait SecondaryStoresConfig {
  def lookup(storeId: String): Option[SecondaryConfigInfo]
  def create(secondaryInfo: SecondaryConfigInfo): SecondaryConfigInfo
  def updateNextRunTime(storeId: String, newNextRunTime: DateTime): Unit
}
