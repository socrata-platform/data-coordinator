package com.socrata.datacoordinator.backup

class Resync(msg: String) extends RuntimeException(msg)

object Resync {
  def apply[T](dataset: T, explanation: String)(implicit ev: DatasetIdable[T]): Nothing =
    throw new Resync("Dataset " + ev.datasetId(dataset).underlying + ": " + explanation)

  @inline final def unless[T: DatasetIdable](dataset: T, condition: Boolean, explanation: => String) {
    if(!condition) apply(dataset, explanation)
  }
}
