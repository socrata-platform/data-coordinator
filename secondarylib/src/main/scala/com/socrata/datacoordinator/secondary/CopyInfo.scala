package com.socrata.datacoordinator.secondary

import com.socrata.datacoordinator.id.CopyId
import org.joda.time.DateTime

case class CopyInfo(systemId: CopyId, copyNumber: Long, lifecycleStage: LifecycleStage, dataVersion: Long, lastModified: DateTime)
