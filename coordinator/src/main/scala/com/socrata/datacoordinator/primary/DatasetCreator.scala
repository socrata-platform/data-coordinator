package com.socrata.datacoordinator.primary

import com.socrata.soql.brita.AsciiIdentifierFilter

class DatasetCreator[CT, CV](mutator: DatabaseMutator[CT, CV], systemColumns: Map[String, CT], idColumnName: String) {
  def createDataset(datasetId: String, username: String) {
    mutator.withTransaction() { providerOfNecessaryThings =>
      import providerOfNecessaryThings._
      val table = datasetMap.create(datasetId, "t")
      val logger = datasetLog(table.datasetInfo)
      val loader = schemaLoader(table, logger)

      loader.create(table)

      for((name, typ) <- systemColumns) {
        val col = datasetMap.addColumn(table, name, nameForType(typ), AsciiIdentifierFilter(List("s", name)).toLowerCase)
        loader.addColumn(col)
        if(col.logicalName == idColumnName) {
          val newCol = datasetMap.setSystemPrimaryKey(col)
          loader.makeSystemPrimaryKey(newCol)
        }
      }

      val newVersion = logger.endTransaction().getOrElse(sys.error(s"No record of the `working copy created' or ${systemColumns.size} columns?"))
      datasetMap.updateDataVersion(table, newVersion)
      globalLog.log(table.datasetInfo, newVersion, now, username)
    }
  }
}
