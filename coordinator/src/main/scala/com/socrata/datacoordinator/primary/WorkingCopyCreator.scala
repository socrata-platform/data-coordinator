package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.truth.metadata.{CopyPair, VersionInfo}

class WorkingCopyCreator[CT, CV](mutator: DatabaseMutator[CT, CV], systemColumns: Map[String, CT], idColumnName: String) {
  def copyDataset(datasetId: String, username: String, copyData: Boolean): VersionInfo = {
    mutator.withSchemaUpdate(datasetId, username) { providerOfNecessaryThings =>
      import providerOfNecessaryThings._

      datasetMap.ensureUnpublishedCopy(datasetInfo) match {
        case Left(existing) =>
          assert(existing == tableInfo)
          existing
        case Right(CopyPair(oldTable, newTable)) =>
          assert(oldTable == tableInfo)
          schemaLoader.create(newTable)
          val schema = datasetMap.schema(newTable)
          for(ci <- schema.values) {
            schemaLoader.addColumn(ci)
            if(ci.logicalName == idColumnName) schemaLoader.makeSystemPrimaryKey(ci)
            else if(ci.isUserPrimaryKey) schemaLoader.makePrimaryKey(ci)
          }
          if(copyData) {
            datasetContentsCopier.copy(oldTable, newTable, schema)
          }
          newTable
      }
    }
  }
}
