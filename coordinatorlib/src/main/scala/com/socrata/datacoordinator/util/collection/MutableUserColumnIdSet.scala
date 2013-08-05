package com.socrata.datacoordinator.util.collection

import com.socrata.datacoordinator.id.UserColumnId

object MutableUserColumnIdSet {
  def apply(xs: UserColumnId*): MutableUserColumnIdSet = {
    val tmp = new java.util.HashSet[String]
    xs.foreach { x => tmp.add(x.underlying) }
    new MutableUserColumnIdSet(tmp)
  }

  val empty = apply()
}

class MutableUserColumnIdSet (private var _underlying: java.util.Set[String]) extends (UserColumnId => Boolean) {
  def underlying = _underlying

  def apply(x: UserColumnId) = underlying.contains(x.underlying)

  def contains(x: UserColumnId) = underlying.contains(x.underlying)

  def iterator: Iterator[UserColumnId] = new Iterator[UserColumnId] {
    val it = underlying.iterator
    def hasNext = it.hasNext
    def next() = new UserColumnId(it.next())
  }

  @inline
  def foreach[U](f: UserColumnId => U): Unit = {
    val it = underlying.iterator
    while(it.hasNext) f(new UserColumnId(it.next()))
  }

  def freeze() = {
    if(underlying == null) throw new NullPointerException
    val result = new UserColumnIdSet(underlying)
    _underlying = null
    result
  }

  def ++=(x: UserColumnIdSet) {
    underlying.addAll(x.unsafeUnderlying)
  }

  def +=(x: UserColumnId) {
    underlying.add(x.underlying)
  }

  def -=(x: UserColumnId) {
    underlying.remove(x.underlying)
  }

  def size = underlying.size

  def isEmpty = underlying.isEmpty
  def nonEmpty = !underlying.isEmpty

  def clear() {
    underlying.clear()
  }

  def toSet = {
    val b = Set.newBuilder[UserColumnId]
    val it = underlying.iterator
    while(it.hasNext) {
      b += new UserColumnId(it.next())
    }
    b.result()
  }

  override def hashCode = underlying.hashCode
  override def equals(o: Any) = o match {
    case that: MutableUserColumnIdSet => this.underlying == that.underlying
    case _ => false
  }

  override def toString = underlying.toString
}
