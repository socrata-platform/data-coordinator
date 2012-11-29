package com.socrata.datacoordinator.truth.metadata

case class VersionInfo(tableInfo: TableInfo, logicalVersion: Long, lifecycleStage: LifecycleStage, userPrimaryKey: Option[String])
