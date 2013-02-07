package com.socrata.datacoordinator.util

import java.io.Closeable

trait CloseableIterator[+T] extends Iterator[T] with Closeable

object CloseableIterator {
  val empty: CloseableIterator[Nothing] = new CloseableIterator[Nothing] {
    def hasNext = false
    def next() = Iterator.empty.next()
    def close() {}
  }

  def single[T](t: T): CloseableIterator[T] = new CloseableIterator[T] {
    var hasNext = true

    def next() =
      if(hasNext) {
        hasNext = false
        t
      } else {
        Iterator.empty.next()
      }

    def close() {}
  }

  def simple[T](it: Iterator[T]) = new CloseableIterator[T] {
    def hasNext = it.hasNext
    def next() = it.next()
    def close() {}
  }
}
