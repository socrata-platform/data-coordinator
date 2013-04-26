package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.DatasetMutator
import com.socrata.soql.environment.ColumnName
import com.socrata.datacoordinator.truth.universe.{DatasetMutatorProvider, Universe}
import com.rojoma.simplearm.Managed

class ColumnAdder[CT](universe: Managed[Universe[CT, _] with DatasetMutatorProvider],
                      physicalColumnBaseBase: (ColumnName, Boolean) => String)
  extends ExistingDatasetMutator
{
  def addToSchema(dataset: String, columns: Map[ColumnName, CT], username: String): Map[ColumnName, ColumnInfo[CT]] = {
    def columnCreations[CV](ctx: DatasetMutator[CT, CV]#MutationContext) = columns.iterator.map { case (columnName, columnType) =>
      val baseName = physicalColumnBaseBase(columnName, false)
      ctx.addColumn(columnName, columnType, baseName)
    }.toList

    finish(dataset) {
      for {
        u <- universe
        ctxOpt <- u.datasetMutator.openDataset(as = username)(dataset, _ => ())
        ctx <- ctxOpt
      } yield {
        columnCreations(ctx).foldLeft(Map.empty[ColumnName, ColumnInfo[CT]]) { (acc, ci) =>
          acc + (ci.logicalName -> ci)
        }
      }
    }
  }
}
