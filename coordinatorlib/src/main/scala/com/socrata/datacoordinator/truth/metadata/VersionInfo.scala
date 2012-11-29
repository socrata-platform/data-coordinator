package com.socrata.datacoordinator.truth.metadata

case class VersionInfo(tableInfo: TableInfo, lifecycleVersion: Long, lifecycleStage: LifecycleStage, userPrimaryKey: Option[String])
