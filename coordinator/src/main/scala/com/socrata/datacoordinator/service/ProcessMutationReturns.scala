package com.socrata.datacoordinator.service

import org.joda.time.DateTime

case class ProcessMutationReturns(copyNumber: Long, dataVersion: Long, lastModified: DateTime, commandResult: Seq[MutationScriptCommandResult])
