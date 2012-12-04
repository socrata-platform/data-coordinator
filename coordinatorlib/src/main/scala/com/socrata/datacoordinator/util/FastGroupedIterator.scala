package com.socrata.datacoordinator.util

import scala.collection.mutable

class FastGroupedIterator[T](underlying: Iterator[T], size: Int) extends Iterator[Seq[T]] {
  def hasNext = underlying.hasNext

  def next(): Seq[T] = {
    val x = new mutable.ArrayBuffer[T](size)
    while(x.length < size) {
      x += underlying.next()
      if(!underlying.hasNext) return x
    }
    x
  }
}
