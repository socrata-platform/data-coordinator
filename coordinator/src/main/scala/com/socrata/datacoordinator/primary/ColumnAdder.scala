package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.DataWritingContext
import com.socrata.soql.environment.ColumnName

class ColumnAdder[CT] private (val dataContext: DataWritingContext) extends ExistingDatasetMutator {
  import dataContext.datasetMutator._

  def addToSchema(dataset: String, columns: Map[ColumnName, CT], username: String): Map[ColumnName, ColumnInfo] = {
    def columnCreations(ctx: MutationContext) = columns.iterator.map { case (columnName, columnType) =>
      val baseName = dataContext.physicalColumnBaseBase(columnName)
      ctx.addColumn(columnName, dataContext.typeContext.nameFromType(columnType.asInstanceOf[dataContext.CT] /* SI-5712, see below */), baseName)
    }.toList

    finish(dataset) {
      for {
        ctxOpt <- openDataset(as = username)(dataset)
        ctx <- ctxOpt
      } yield {
        columnCreations(ctx).foldLeft(Map.empty[ColumnName, ColumnInfo]) { (acc, ci) =>
          acc + (ci.logicalName -> ci)
        }
      }
    }
  }
}

object ColumnAdder {
  // cannot pass the evidence into ColumnAdder's ctor because of SI-5712.  So instead just
  // _require_ the evidence and then do a manual cast, which the evidence we're given
  // has proven is safe.  This is also why ColumnAdder's ctor is private; once that bug
  // is fixed this companion object can go away.
  def apply[CT](dataContext: DataWritingContext)(implicit ev: CT =:= dataContext.CT) = new ColumnAdder[CT](dataContext)
}
