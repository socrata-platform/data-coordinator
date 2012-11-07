package com.socrata.datacoordinator.loader

trait RowIdMap[CV, T] {
  def put(x: CV, v: T)
  def apply(x: CV): T
  def contains(x: CV): Boolean
}
