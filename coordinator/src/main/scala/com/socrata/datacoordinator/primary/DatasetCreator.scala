package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.truth.universe.{DatasetMutatorProvider, Universe}
import com.socrata.soql.environment.ColumnName
import com.rojoma.simplearm.Managed

class DatasetCreator[CT](universe: Managed[Universe[CT, _] with DatasetMutatorProvider],
                         systemSchema: Map[ColumnName, CT],
                         systemColumnIdName: ColumnName,
                         physicalColumnBaseBase: (ColumnName, Boolean) => String) {
  def createDataset(datasetId: String, username: String, localeName: String) {
    for {
      u <- universe
      ctxOpt <- u.datasetMutator.createDataset(as = username)(datasetId, "t", localeName)
      ctx <- ctxOpt
    } {
      systemSchema.foreach { case (name, typ) =>
        val col = ctx.addColumn(name, typ, physicalColumnBaseBase(name, true))
        if(name == systemColumnIdName) ctx.makeSystemPrimaryKey(col)
      }
    }
  }
}
