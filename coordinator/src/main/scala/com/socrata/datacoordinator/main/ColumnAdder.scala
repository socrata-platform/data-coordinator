package com.socrata.datacoordinator.main

import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.util.collection.ColumnIdMap

class ColumnAdder[CT, CV](mutator: DatabaseMutator[CT, CV]) {
  // Glue points we want/need
  //
  // Data updates (schema changes, upsert, etc)
  // Global log listener (specifically: a playback in some postgres table)
  // A secondary store (just a dummy for plugging in)
  // Store-update operations
  //  * Refresh dataset X to {StoreSet} : Future[Either[Error, NewVersion]]
  //  * Remove dataset X from {StoreSet} : Future[Option[Error]]
  // Get replication status : Map[Store, Version] (from secondary manifest)

  def addToSchema(dataset: String, columns: Map[String, CT], username: String): Map[String, ColumnInfo] = {
    mutator.withTransaction() { providerOfNecessaryThings =>
      import providerOfNecessaryThings._
      val ds = datasetMapWriter.datasetInfo(dataset).getOrElse(sys.error("Augh no such dataset"))
      val table = datasetMapWriter.latest(ds)
      val logger = datasetLog(ds)

      var result = Map.empty[String, ColumnInfo]
      for((columnName, columnType) <- columns) {
        val baseName = physicalColumnBaseForType(columnType)
        val col = datasetMapWriter.addColumn(table, columnName, nameForType(columnType), baseName)
        schemaLoader(col.versionInfo, logger).addColumn(col)
        result += columnName -> col
      }

      logger.endTransaction().foreach { ver =>
        truthManifest.updateLatestVersion(ds, ver)
        globalLog.log(ds, ver, now, username)
      }

      result
    }
  }
}
