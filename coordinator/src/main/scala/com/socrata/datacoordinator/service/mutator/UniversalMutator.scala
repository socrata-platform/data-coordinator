package com.socrata.datacoordinator.service.mutator

import com.rojoma.json.ast.JValue
import com.socrata.datacoordinator.id.DatasetId
import org.joda.time.DateTime
import com.rojoma.simplearm.{SimpleArm, Managed}
import com.rojoma.json.io.JsonReader
import java.nio.charset.StandardCharsets
import com.socrata.datacoordinator.util.IndexedTempFile

// These are all called "universal" because they provide their own Universe

trait UniversalDatasetCreator {
  def createScript(commandStream: Iterator[JValue]): (DatasetId, Long, DateTime, Seq[MutationScriptCommandResult])
}

trait UniversalDatasetCopier {
  def copyOp(datasetId: DatasetId, command: JValue): (Long, DateTime)
}

trait UniversalDDL {
  def ddlScript(udatasetId: DatasetId, commandStream: Iterator[JValue]): (Long, DateTime, Seq[MutationScriptCommandResult])
}

trait UniversalUpserter {
  def upsertScript(datasetId: DatasetId, commandStream: Iterator[JValue], output: IndexedTempFile): (Long, DateTime)
}

trait UniversalMutator extends UniversalDatasetCreator with UniversalDatasetCopier with UniversalDDL with UniversalUpserter
