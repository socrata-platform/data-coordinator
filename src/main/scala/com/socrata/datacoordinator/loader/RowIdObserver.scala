package com.socrata.datacoordinator.loader

trait RowIdObserver[CV] {
  def observe(x: CV)
  def observed(x: CV): Boolean
  def clear()
}
