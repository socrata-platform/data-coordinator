package com.socrata.datacoordinator.service

import com.socrata.datacoordinator.id.DatasetId
import org.joda.time.DateTime

case class ProcessCreationReturns(datasetId: DatasetId, dataVersion: Long, lastModified: DateTime, commandResult: Seq[MutationScriptCommandResult])
