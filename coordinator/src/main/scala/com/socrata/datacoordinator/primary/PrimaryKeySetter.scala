package com.socrata.datacoordinator.primary

import com.rojoma.simplearm.Managed

import com.socrata.soql.environment.ColumnName
import com.socrata.datacoordinator.truth.universe.{DatasetMutatorProvider, Universe}
import com.socrata.datacoordinator.id.DatasetId

class PrimaryKeySetter[CT](universe: Managed[Universe[CT, _] with DatasetMutatorProvider]) extends ExistingDatasetMutator {
  def makePrimaryKey(dataset: DatasetId, column: ColumnName, username: String) {
    finish(dataset) {
      for {
        u <- universe
        ctxOpt <- u.datasetMutator.openDataset(as = username)(dataset, _ => ())
        ctx <- ctxOpt
      } yield {
        import ctx._
        schema.values.find(_.logicalName == column) match {
          case Some(c) =>
            makeUserPrimaryKey(c)
          case None => sys.error("No such column") // TODO: better error
        }
      }
    }
  }
}
