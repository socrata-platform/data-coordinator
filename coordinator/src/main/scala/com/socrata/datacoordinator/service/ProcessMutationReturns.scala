package com.socrata.datacoordinator.service

import org.joda.time.DateTime

case class ProcessMutationReturns(copyNumber: Long, dataVersion: Long, dataShapeVersion: Long, lastModified: DateTime, commandResult: Seq[MutationScriptCommandResult])
