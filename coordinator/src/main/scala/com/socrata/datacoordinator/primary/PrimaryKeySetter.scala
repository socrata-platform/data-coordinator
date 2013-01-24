package com.socrata.datacoordinator.primary

class PrimaryKeySetter[CT, CV](mutator: DatabaseMutator[CT, CV]) {
  def makePrimaryKey(dataset: String, column: String, username: String) {
    mutator.withTransaction() { providerOfNecessaryThings =>
      import providerOfNecessaryThings._
      val ds = datasetMap.datasetInfo(dataset).getOrElse(sys.error("Augh no such dataset"))
      val table = datasetMap.latest(ds)
      val schema = datasetMap.schema(table)

      val col = schema.values.iterator.find(_.logicalName == column).getOrElse {
        sys.error("No such column")
      }

      val logger = datasetLog(ds)

      schemaLoader(table, logger).makePrimaryKey(col)
      datasetMap.setUserPrimaryKey(col)

      logger.endTransaction().foreach { ver =>
        datasetMap.updateDataVersion(table, ver)
        globalLog.log(ds, ver, now, username)
      }
    }
  }
}
