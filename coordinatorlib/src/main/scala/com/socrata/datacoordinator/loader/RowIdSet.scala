package com.socrata.datacoordinator.loader

trait RowIdSet[CV] {
  def add(x: CV)
  def remove(x: CV)
  def apply(x: CV): Boolean
  def clear()
  def iterator: Iterator[CV]
}
