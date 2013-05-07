package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.truth.universe.{DatasetMutatorProvider, Universe}
import com.socrata.soql.environment.ColumnName
import com.rojoma.simplearm.Managed
import com.socrata.datacoordinator.id.DatasetId

class DatasetCreator[CT](universe: Managed[Universe[CT, _] with DatasetMutatorProvider],
                         systemSchema: Map[ColumnName, CT],
                         systemIdColumnName: ColumnName,
                         versionColumnName: ColumnName,
                         physicalColumnBaseBase: (ColumnName, Boolean) => String) {
  def createDataset(username: String, localeName: String): DatasetId = {
    for {
      u <- universe
      ctx <- u.datasetMutator.createDataset(as = username)(localeName)
    } yield {
      systemSchema.foreach { case (name, typ) =>
        val col = ctx.addColumn(name, typ, physicalColumnBaseBase(name, true))
        val col2 =
          if(name == systemIdColumnName) ctx.makeSystemPrimaryKey(col)
          else col
        if(name == versionColumnName) ctx.makeVersion(col2)
      }
      ctx.copyInfo.datasetInfo.systemId
    }
  }
}
