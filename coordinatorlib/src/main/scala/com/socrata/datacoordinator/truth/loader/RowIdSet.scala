package com.socrata.datacoordinator
package truth.loader

trait RowIdSet[CV] {
  def add(x: CV)
  def remove(x: CV)
  def apply(x: CV): Boolean
  def clear()
  def iterator: Iterator[CV]
}
