package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.truth.metadata.{CopyPair, CopyInfo}

class WorkingCopyCreator[CT, CV](mutator: DatabaseMutator[CT, CV], idColumnName: String) {
  def copyDataset(datasetId: String, username: String, copyData: Boolean): CopyInfo = {
    mutator.withSchemaUpdate(datasetId, username) { providerOfNecessaryThings =>
      import providerOfNecessaryThings._

      datasetMap.ensureUnpublishedCopy(initialDatasetInfo) match {
        case Left(existing) =>
          assert(existing == initialCopyInfo)
          existing
        case Right(copyPair@CopyPair(oldTable, newTable)) =>
          assert(oldTable == initialCopyInfo)

          // Great.  Now we can actually do the data loading.
          schemaLoader.create(newTable)
          val oldPrimaryKey = datasetMap.schema(oldTable).values.find(_.isUserPrimaryKey).map(_.systemId)
          val schema = datasetMap.schema(newTable)
          for(ci <- schema.values) {
            schemaLoader.addColumn(ci)
            if(ci.logicalName == idColumnName) {
              schemaLoader.makeSystemPrimaryKey(ci)
              datasetMap.setSystemPrimaryKey(ci)
            } else if(oldPrimaryKey.isDefined && ci.systemId == oldPrimaryKey.get) {
              schemaLoader.makePrimaryKey(ci)
              datasetMap.setUserPrimaryKey(ci)
            }
          }

          if(copyData) {
            datasetContentsCopier.copy(oldTable, newTable, schema)
          }
          newTable
      }
    }
  }
}
