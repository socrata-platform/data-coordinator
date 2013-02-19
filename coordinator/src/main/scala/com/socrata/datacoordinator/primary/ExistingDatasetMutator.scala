package com.socrata.datacoordinator.primary

import scalaz._
import scalaz.effect._
import Scalaz._

class ExistingDatasetMutator {
  def finish[A](dataset: String)(f: Option[A]): IO[A] = f match {
    case Some(a) => a.pure[IO]
    case None => IO(sys.error("No such dataset " + dataset)) // TODO better error
  }
}
