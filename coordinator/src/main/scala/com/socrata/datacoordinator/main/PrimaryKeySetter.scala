package com.socrata.datacoordinator.main

import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.util.collection.ColumnIdMap

class PrimaryKeySetter[CT, CV](mutator: DatabaseMutator[CT, CV]) {
  def makePrimaryKey(dataset: String, column: String, username: String) {
    mutator.withTransaction() { providerOfNecessaryThings =>
      import providerOfNecessaryThings._
      val ds = datasetMapWriter.datasetInfo(dataset).getOrElse(sys.error("Augh no such dataset"))
      val table = datasetMapWriter.latest(ds)
      val schema = datasetMapWriter.schema(table)

      val col = schema.values.iterator.find(_.logicalName == column).getOrElse {
        sys.error("No such column")
      }

      val logger = datasetLog(ds)

      schemaLoader(table, logger).makePrimaryKey(col)
      datasetMapWriter.setUserPrimaryKey(col)

      logger.endTransaction().foreach { ver =>
        truthManifest.updateLatestVersion(ds, ver)
        globalLog.log(ds, ver, now, username)
      }
    }
  }
}
