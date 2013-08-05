package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.DatasetMutator
import com.socrata.soql.environment.ColumnName
import com.socrata.datacoordinator.truth.universe.{DatasetMutatorProvider, Universe}
import com.rojoma.simplearm.Managed
import com.socrata.datacoordinator.id.{UserColumnId, DatasetId}
import com.socrata.datacoordinator.util.collection.{MutableUserColumnIdMap, UserColumnIdMap}

class ColumnAdder[CT](universe: Managed[Universe[CT, _] with DatasetMutatorProvider],
                      physicalColumnBaseBase: (String, Boolean) => String)
  extends ExistingDatasetMutator
{
  def addToSchema(dataset: DatasetId, columns: Map[String, CT], genColumnId: String => UserColumnId, username: String): UserColumnIdMap[ColumnInfo[CT]] = {
    def columnCreations[CV](ctx: DatasetMutator[CT, CV]#MutationContext) = columns.iterator.map { case (columnName, columnType) =>
      val baseName = physicalColumnBaseBase(columnName, false)
      ctx.addColumn(genColumnId(columnName), columnType, baseName)
    }.toList

    finish(dataset) {
      for {
        u <- universe
        ctxOpt <- u.datasetMutator.openDataset(as = username)(dataset, _ => ())
        ctx <- ctxOpt
      } yield {
        val res = MutableUserColumnIdMap[ColumnInfo[CT]]()
        for(ci <- columnCreations(ctx)) {
          res += ci.userColumnId -> ci
        }
        res.freeze()
      }
    }
  }
}
