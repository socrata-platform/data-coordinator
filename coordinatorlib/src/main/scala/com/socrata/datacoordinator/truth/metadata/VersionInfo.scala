package com.socrata.datacoordinator.truth.metadata

case class VersionInfo(tableInfo: TableInfo, systemId: Long, lifecycleVersion: Long, lifecycleStage: LifecycleStage)
