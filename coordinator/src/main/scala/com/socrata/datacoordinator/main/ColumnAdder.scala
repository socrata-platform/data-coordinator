package com.socrata.datacoordinator.main

abstract class ColumnAdder[CT, CV](mutator: DatabaseMutator[CT, CV]) {
  // Glue points we want/need
  //
  // Data updates (schema changes, upsert, etc)
  // Global log listener (specifically: a playback in some postgres table)
  // A secondary store (just a dummy for plugging in)
  // Store-update operations
  //  * Refresh dataset X to {StoreSet} : Future[Either[Error, NewVersion]]
  //  * Remove dataset X from {StoreSet} : Future[Option[Error]]
  // Get replication status : Map[Store, Version] (from secondary manifest)

  def addToSchema(dataset: String, columnName: String, columnType: CT, username: String) {
    mutator.withTransaction() { providerOfNecessaryThings =>
      import providerOfNecessaryThings._
      val ds = datasetMapWriter.datasetInfo(dataset).getOrElse(sys.error("Augh no such dataset"))
      val table = datasetMapWriter.latest(ds)
      val baseName = physicalColumnBaseForType(columnType) + "_" + singleId()
      val col = datasetMapWriter.addColumn(table, columnName, nameForType(columnType), baseName)
      val logger = datasetLog(ds)

      loader(col.versionInfo).addColumn(col)
      logger.columnCreated(col)

      logger.endTransaction().foreach { ver =>
        truthManifest.updateLatestVersion(ds, ver)
        globalLog.log(ds, ver, now, username)
      }
    }
  }
}
