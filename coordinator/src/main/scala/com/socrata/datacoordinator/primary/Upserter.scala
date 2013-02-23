package com.socrata.datacoordinator.primary

import scalaz._
import scalaz.effect._
import Scalaz._

import com.rojoma.simplearm.SimpleArm

import com.socrata.datacoordinator.Row
import com.socrata.datacoordinator.truth.loader.Report
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.MonadicDatasetMutator

class Upserter[CV](mutator: MonadicDatasetMutator[CV]) extends ExistingDatasetMutator {
  import mutator.{upsert => upsrt, _}
  def upsert(dataset: String, username: String)(inputGenerator: ColumnIdMap[ColumnInfo] => IO[Iterator[Either[CV, Row[CV]]]]): IO[Report[CV]] =
    mutator.withDataset(as = username)(dataset) {
      schema.flatMap { s =>
        upsrt(inputGenerator(s))
      }
    }.flatMap(finish(dataset))
}
