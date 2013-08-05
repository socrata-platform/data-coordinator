package com.socrata.datacoordinator.util.collection

import com.socrata.datacoordinator.id.UserColumnId

object UserColumnIdSet {
  def apply(xs: UserColumnId*): UserColumnIdSet = {
    val tmp = new java.util.HashSet[String]
    xs.foreach { x => tmp.add(x.underlying) }
    new UserColumnIdSet(tmp)
  }

  val empty = apply()
}

class UserColumnIdSet(val unsafeUnderlying: java.util.Set[String]) extends (UserColumnId => Boolean) {
  def apply(x: UserColumnId) = unsafeUnderlying.contains(x.underlying)

  def contains(x: UserColumnId) = unsafeUnderlying.contains(x.underlying)

  def iterator: Iterator[UserColumnId] = new Iterator[UserColumnId] {
    val it = unsafeUnderlying.iterator
    def hasNext = it.hasNext
    def next() = new UserColumnId(it.next())
  }

  def intersect(that: UserColumnIdSet): UserColumnIdSet = {
    if(this.unsafeUnderlying.size <= that.unsafeUnderlying.size) {
      val filter = that.unsafeUnderlying
      val it = unsafeUnderlying.iterator
      val target = new java.util.HashSet[String]
      while(it.hasNext) {
        val elem = it.next()
        if(filter.contains(elem)) target.add(elem)
      }
      new UserColumnIdSet(target)
    } else {
      that.intersect(this)
    }
  }

  def -(x: UserColumnId) = {
    val copy = new java.util.HashSet(unsafeUnderlying)
    copy.remove(x.underlying)
    new UserColumnIdSet(copy)
  }

  def --(xs: UserColumnIdSet) = {
    val copy = new java.util.HashSet(unsafeUnderlying)
    copy.removeAll(xs.unsafeUnderlying)
    new UserColumnIdSet(copy)
  }

  def foreach[U](f: UserColumnId => U) {
    val it = unsafeUnderlying.iterator
    while(it.hasNext) {
      f(new UserColumnId(it.next()))
    }
  }

  def size = unsafeUnderlying.size

  def isEmpty = unsafeUnderlying.isEmpty
  def nonEmpty = !unsafeUnderlying.isEmpty

  def filter(f: UserColumnId => Boolean) = {
    val result = new java.util.HashSet[String]
    val it = unsafeUnderlying.iterator
    while(it.hasNext) {
      val l = it.next()
      if(f(new UserColumnId(l))) result.add(l)
    }
    new UserColumnIdSet(result)
  }

  def filterNot(f: UserColumnId => Boolean) = {
    val result = new java.util.HashSet[String]
    val it = unsafeUnderlying.iterator
    while(it.hasNext) {
      val l = it.next()
      if(!f(new UserColumnId(l))) result.add(l)
    }
    new UserColumnIdSet(result)
  }

  def partition(f: UserColumnId => Boolean) = {
    val yes = new java.util.HashSet[String]
    val no = new java.util.HashSet[String]
    val it = unsafeUnderlying.iterator
    while(it.hasNext) {
      val l = it.next()
      if(f(new UserColumnId(l))) yes.add(l)
      else no.add(l)
      true
    }
    (new UserColumnIdSet(yes), new UserColumnIdSet(no))
  }

  def toSet = {
    val b = Set.newBuilder[UserColumnId]
    val it = unsafeUnderlying.iterator
    while(it.hasNext) {
      b += new UserColumnId(it.next())
    }
    b.result()
  }

  override def hashCode = unsafeUnderlying.hashCode
  override def equals(o: Any) = o match {
    case that: UserColumnIdSet => this.unsafeUnderlying == that.unsafeUnderlying
    case _ => false
  }

  override def toString = unsafeUnderlying.toString
}
