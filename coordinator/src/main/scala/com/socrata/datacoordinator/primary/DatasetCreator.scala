package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.truth.universe.{DatasetMutatorProvider, Universe}
import com.socrata.soql.environment.ColumnName
import com.rojoma.simplearm.Managed
import com.socrata.datacoordinator.id.{UserColumnId, DatasetId}
import com.socrata.datacoordinator.util.collection.UserColumnIdMap

class DatasetCreator[CT](universe: Managed[Universe[CT, _] with DatasetMutatorProvider],
                         systemSchema: UserColumnIdMap[CT],
                         systemIdColumnId: UserColumnId,
                         versionColumnId: UserColumnId,
                         physicalColumnBaseBase: (String, Boolean) => String) {
  def createDataset(username: String, localeName: String): DatasetId = {
    for {
      u <- universe
      ctx <- u.datasetMutator.createDataset(as = username)(localeName)
    } yield {
      systemSchema.foreach { (cid, typ) =>
        val col = ctx.addColumn(cid, typ, physicalColumnBaseBase(cid.underlying, true))
        val col2 =
          if(cid == systemIdColumnId) ctx.makeSystemPrimaryKey(col)
          else col
        if(cid == versionColumnId) ctx.makeVersion(col2)
      }
      ctx.copyInfo.datasetInfo.systemId
    }
  }
}
