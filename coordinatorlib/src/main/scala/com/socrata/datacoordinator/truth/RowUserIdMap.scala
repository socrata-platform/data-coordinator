package com.socrata.datacoordinator.truth

import scala.collection.JavaConverters._

trait RowUserIdMap[CV, T] {
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

class SimpleRowUserIdMap[CV, T] extends RowUserIdMap[CV, T] {
  val map = new java.util.HashMap[CV, T]

  def put(x: CV, v: T) { map.put(x, v) }

  def apply(x: CV): T = {
    if(!map.containsKey(x)) throw new NoSuchElementException
    map.get(x)
  }

  def get(x: CV): Option[T] =
    if(!map.containsKey(x)) None
    else Some(map.get(x))

  def clear() { map.clear() }

  def contains(x: CV): Boolean = map.containsKey(x)

  def isEmpty: Boolean = map.isEmpty

  def size: Int = map.size

  def foreach(f: (CV, T) => Unit) {
    val it = map.entrySet().iterator
    while(it.hasNext) {
      val ent = it.next()
      f(ent.getKey, ent.getValue)
    }
  }

  def valuesIterator: Iterator[T] = map.values.iterator.asScala
}
