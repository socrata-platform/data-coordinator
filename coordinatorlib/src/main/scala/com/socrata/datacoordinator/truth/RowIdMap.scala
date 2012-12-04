package com.socrata.datacoordinator.truth

trait RowIdMap[CV, T] {
  def put(x: CV, v: T)
  def apply(x: CV): T
  def get(x: CV): Option[T]
  def clear()
  def contains(x: CV): Boolean
  def isEmpty: Boolean
  def size: Int
  def foreach(f: (CV, T) => Unit)
  def valuesIterator: Iterator[T]
}
