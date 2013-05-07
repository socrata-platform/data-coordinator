package com.socrata.datacoordinator.primary

import com.rojoma.simplearm.Managed

import com.socrata.datacoordinator.Row
import com.socrata.datacoordinator.truth.loader.Report
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.universe.{DatasetMutatorProvider, Universe}
import com.socrata.datacoordinator.id.DatasetId

class Upserter[CT, CV](universe: Managed[Universe[CT, CV] with DatasetMutatorProvider]) extends ExistingDatasetMutator {
  def upsert(dataset: DatasetId, username: String)(inputGenerator: ColumnIdMap[ColumnInfo[CT]] => Iterator[Either[CV, Row[CV]]], replaceUpdatedRows: Boolean = true): Report[CV] =
    finish(dataset) {
      for {
        u <- universe
        ctxOpt <- u.datasetMutator.openDataset(as = username)(dataset, _ => ())
        ctx <- ctxOpt
      } yield {
        ctx.upsert(inputGenerator(ctx.schema).zipWithIndex.map {
          case (Left(id), num) => ctx.DeleteJob(num, id, None)
          case (Right(row), num) => ctx.UpsertJob(num, row)
        },
        replaceUpdatedRows)
      }
    }
}
