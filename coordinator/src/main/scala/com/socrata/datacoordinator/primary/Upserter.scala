package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.Row
import com.socrata.datacoordinator.truth.loader.Report
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.MonadicDatasetMutator

class Upserter[CV](mutator: MonadicDatasetMutator[CV]) extends ExistingDatasetMutator {
  def upsert(dataset: String, username: String)(inputGenerator: ColumnIdMap[ColumnInfo] => Iterator[Either[CV, Row[CV]]]): Report[CV] =
    finish(dataset) {
      mutator.withDataset(as = username)(dataset) { ctx =>
        ctx.upsert(inputGenerator(ctx.schema))
      }
    }
}
