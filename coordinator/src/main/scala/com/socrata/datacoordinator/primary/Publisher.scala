package com.socrata.datacoordinator.primary

import scalaz._
import scalaz.effect._
import Scalaz._

import com.socrata.datacoordinator.truth.MonadicDatasetMutator
import com.socrata.datacoordinator.truth.metadata.CopyInfo

class Publisher[CT](mutator: MonadicDatasetMutator[CT]) extends ExistingDatasetMutator {
  import mutator.{publish => pblsh, _}

  def publish(dataset: String, username: String): IO[CopyInfo] = {
    withDataset(as = username)(dataset) {
      pblsh
    }.flatMap(finish(dataset))
  }
}
