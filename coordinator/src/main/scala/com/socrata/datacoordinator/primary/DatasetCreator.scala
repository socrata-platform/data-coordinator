package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.truth.universe.{DatasetMutatorProvider, Universe}
import com.socrata.soql.environment.ColumnName
import com.rojoma.simplearm.Managed
import com.socrata.datacoordinator.id.DatasetId

class DatasetCreator[CT](universe: Managed[Universe[CT, _] with DatasetMutatorProvider],
                         systemSchema: Map[ColumnName, CT],
                         systemColumnIdName: ColumnName,
                         physicalColumnBaseBase: (ColumnName, Boolean) => String) {
  def createDataset(username: String, localeName: String): DatasetId = {
    for {
      u <- universe
      ctx <- u.datasetMutator.createDataset(as = username)(localeName)
    } yield {
      systemSchema.foreach { case (name, typ) =>
        val col = ctx.addColumn(name, typ, physicalColumnBaseBase(name, true))
        if(name == systemColumnIdName) ctx.makeSystemPrimaryKey(col)
      }
      ctx.copyInfo.datasetInfo.systemId
    }
  }
}
