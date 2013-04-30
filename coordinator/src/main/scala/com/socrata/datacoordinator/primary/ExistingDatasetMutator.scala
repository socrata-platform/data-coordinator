package com.socrata.datacoordinator.primary

import com.socrata.datacoordinator.id.DatasetId

class ExistingDatasetMutator {
  def finish[A](dataset: DatasetId)(f: Option[A]): A = f match {
    case Some(a) => a
    case None => sys.error("No such dataset " + dataset) // TODO better error
  }
}
