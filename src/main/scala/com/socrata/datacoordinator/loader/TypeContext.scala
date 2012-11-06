package com.socrata.datacoordinator.loader

/** Non-dataset-specific operations on column values. */
trait TypeContext[CV] {
  def isNull(value: CV): Boolean
  def makeSystemIdValue(id: Long): CV
  def makeIdObserver(): RowIdObserver[CV]
}
