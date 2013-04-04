package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.Row
import com.socrata.datacoordinator.truth.loader.Report
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.DatasetMutator

class Upserter[CV](mutator: DatasetMutator[_, CV]) extends ExistingDatasetMutator {
  def upsert(dataset: String, username: String)(inputGenerator: ColumnIdMap[ColumnInfo] => Iterator[Either[CV, Row[CV]]]): Report[CV] =
    finish(dataset) {
      for {
        ctxOpt <- mutator.openDataset(as = username)(dataset)
        ctx <- ctxOpt
      } yield {
        ctx.upsert(inputGenerator(ctx.schema).zipWithIndex.map {
          case (Left(id), num) => ctx.DeleteJob(num, id)
          case (Right(row), num) => ctx.UpsertJob(num, row)
        })
      }
    }
}
