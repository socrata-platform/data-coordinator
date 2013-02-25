package com.socrata.datacoordinator.primary

import scalaz._
import scalaz.effect._
import Scalaz._

import com.socrata.datacoordinator.truth.MonadicDatasetMutator

class PrimaryKeySetter(mutator: MonadicDatasetMutator[_]) extends ExistingDatasetMutator {
  import mutator._
  def makePrimaryKey(dataset: String, column: String, username: String): IO[Unit] = {
    withDataset(as = username)(dataset) {
      for {
        s <- schema
        col <- s.values.find(_.logicalName == column) match {
          case Some(c) => c.pure[DatasetM]
          case None => io(sys.error("No such column")) // TODO: better error
        }
        _ <- makeUserPrimaryKey(col)
      } yield ()
    }.flatMap(finish(dataset))
  }
}
