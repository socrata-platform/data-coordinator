package com.socrata.datacoordinator.primary

class ExistingDatasetMutator {
  def finish[A](dataset: String)(f: Option[A]): A = f match {
    case Some(a) => a
    case None => sys.error("No such dataset " + dataset) // TODO better error
  }
}
