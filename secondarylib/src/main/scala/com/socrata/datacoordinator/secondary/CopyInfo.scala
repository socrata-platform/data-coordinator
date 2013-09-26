package com.socrata.datacoordinator.secondary

import com.socrata.datacoordinator.id.CopyId

case class CopyInfo(systemId: CopyId, copyNumber: Long, lifecycleStage: LifecycleStage, dataVersion: Long)
