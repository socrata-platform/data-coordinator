package com.socrata.datacoordinator.main

class DatasetCreator[CT, CV](mutator: DatabaseMutator[CT, CV], systemColumns: Map[String, CT]) {
  def createDataset(datasetId: String, username: String) {
    mutator.withTransaction() { providerOfNecessaryThings =>
      import providerOfNecessaryThings._
      val table = datasetMapWriter.create(datasetId, "t")
      val logger = datasetLog(table.datasetInfo)
      val schemaLoader = loader(table, logger)

      schemaLoader.create(table)
      truthManifest.create(table.datasetInfo)

      for((name, typ) <- systemColumns) {
        val col = datasetMapWriter.addColumn(table, name, nameForType(typ), physicalColumnBaseForType(typ))
        schemaLoader.addColumn(col)
      }

      val newVersion = logger.endTransaction().getOrElse(sys.error(s"No record of the `working copy created' or ${systemColumns.size} columns?"))
      truthManifest.updateLatestVersion(table.datasetInfo, newVersion)
      globalLog.log(table.datasetInfo, newVersion, now, username)
    }
  }
}
