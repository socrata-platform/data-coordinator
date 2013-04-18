package com.socrata.datacoordinator.util

import scala.collection.GenTraversableOnce
import scala.collection.immutable.Queue
import scala.annotation.tailrec

final class BuiltUpIterator[A] private (private var iterators: Queue[() => GenTraversableOnce[A]]) extends Iterator[A] {
  def this(its: Iterator[A]*) = this(Queue(its.map { x => () => x } : _*))
  private var currentIterator: Iterator[A] = null

  override def ++[B >: A](that: => GenTraversableOnce[B]): Iterator[B] = new BuiltUpIterator[B](iterators.enqueue(() => that))

  @tailrec
  def hasNext: Boolean = {
    if(currentIterator == null) {
      if(iterators.isEmpty) return false
      val (newIt, newQ) = iterators.dequeue
      currentIterator = newIt().toIterator
      iterators = newQ
      if(currentIterator.isInstanceOf[BuiltUpIterator[_]]) {
        iterators = currentIterator.asInstanceOf[BuiltUpIterator[A]].iterators.enqueue(iterators)
        currentIterator = Iterator.empty
      }
    }
    if(currentIterator.isEmpty) {
      currentIterator = null
      hasNext
    } else true
  }

  def next(): A =
    if(!hasNext) Iterator.empty.next()
    else currentIterator.next()
}
