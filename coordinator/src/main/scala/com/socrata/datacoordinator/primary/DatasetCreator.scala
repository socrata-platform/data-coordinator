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
      val cols = ctx.addColumns(systemSchema.toSeq.map { case (cid, typ) =>
        ctx.ColumnToAdd(cid, typ, physicalColumnBaseBase(cid.underlying, true))
      })
      cols.foreach { col =>
        val col2 =
          if(col.userColumnId == systemIdColumnId) ctx.makeSystemPrimaryKey(col)
          else col
        if(col2.userColumnId == versionColumnId) ctx.makeVersion(col2)
      }
      ctx.copyInfo.datasetInfo.systemId
    }
  }
}
