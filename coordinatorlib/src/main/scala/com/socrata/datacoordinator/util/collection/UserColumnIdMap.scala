package com.socrata.datacoordinator.util.collection

import scala.collection.JavaConverters._
import scala.annotation.unchecked.uncheckedVariance
import com.socrata.datacoordinator.id.UserColumnId

object UserColumnIdMap {
  def apply[V](kvs: (UserColumnId, V)*): UserColumnIdMap[V] = {
    val tmp = new MutableUserColumnIdMap[V]
    tmp ++= kvs
    tmp.freeze()
  }

  def apply[V](orig: Map[UserColumnId, V]): UserColumnIdMap[V] =
    MutableUserColumnIdMap(orig).freeze()

  private val EMPTY = UserColumnIdMap[Nothing]()

  def empty[V]: UserColumnIdMap[V] = EMPTY
}

class UserColumnIdMap[+V] private[collection](val unsafeUnderlying: java.util.HashMap[String, V @uncheckedVariance]) {
  @inline def size: Int = unsafeUnderlying.size

  @inline def isEmpty: Boolean = unsafeUnderlying.isEmpty

  @inline def nonEmpty: Boolean = !isEmpty

  @inline def contains(t: UserColumnId): Boolean = unsafeUnderlying.containsKey(t.underlying)

  @inline def get(t: UserColumnId): Option[V] = {
    val x = unsafeUnderlying.get(t.underlying)
    if(x.asInstanceOf[AnyRef] eq null) None
    else Some(x)
  }

  @inline def apply(t: UserColumnId): V = {
    val x = unsafeUnderlying.get(t.underlying)
    if(x.asInstanceOf[AnyRef] eq null) throw new NoSuchElementException("No key " + t)
    x
  }

  def iterator = new UserColumnIdMapIterator[V](unsafeUnderlying.entrySet.iterator)

  def ++[V2 >: V](that: UserColumnIdMap[V2]) = {
    val tmp = new java.util.HashMap[String, V2](this.unsafeUnderlying)
    tmp.putAll(that.unsafeUnderlying)
    new UserColumnIdMap[V2](tmp)
  }

  def ++[V2 >: V](that: MutableUserColumnIdMap[V2]) = {
    val tmp = new java.util.HashMap[String, V2](this.unsafeUnderlying)
    tmp.putAll(that.underlying)
    new UserColumnIdMap[V2](tmp)
  }

  def ++[V2 >: V](that: Iterable[(UserColumnId, V2)]) = {
    val tmp = new java.util.HashMap[String, V2](this.unsafeUnderlying)
    for((k, v) <- that) {
      tmp.put(k.underlying, v)
    }
    new UserColumnIdMap[V2](tmp)
  }

  def +[V2 >: V](kv: (UserColumnId, V2)) = {
    val tmp = new java.util.HashMap[String, V2](this.unsafeUnderlying)
    tmp.put(kv._1.underlying, kv._2)
    new UserColumnIdMap[V2](tmp)
  }

  def -(k: UserColumnId) = {
    val tmp = new java.util.HashMap[String, V](this.unsafeUnderlying)
    tmp.remove(k.underlying)
    new UserColumnIdMap[V](tmp)
  }

  @inline def getOrElse[B >: V](k: UserColumnId, v: => B): B = {
    val result = unsafeUnderlying.get(k.underlying)
    if(result == null) v
    else result
  }

  @inline def getOrElseStrict[B >: V](k: UserColumnId, v: B): B = {
    val result = unsafeUnderlying.get(k.underlying)
    if(result == null) v
    else result
  }

  def keys: Iterator[UserColumnId] = iterator.map(_._1)
  def values: Iterable[V] = unsafeUnderlying.values.asScala

  def keySet = new UserColumnIdSet(unsafeUnderlying.keySet)

  def mapValuesStrict[V2](f: V => V2) = {
    val x = new java.util.HashMap[String, V2]
    val it = unsafeUnderlying.entrySet.iterator
    while(it.hasNext) {
      val ent = it.next()
      x.put(ent.getKey, f(ent.getValue))
    }
    new UserColumnIdMap[V2](x)
  }

  def transform[V2](f: (UserColumnId, V) => V2) = {
    val x = new java.util.HashMap[String, V2]
    val it = unsafeUnderlying.entrySet.iterator
    while(it.hasNext) {
      val ent = it.next()
      x.put(ent.getKey, f(new UserColumnId(ent.getKey), ent.getValue))
    }
    new UserColumnIdMap[V2](x)
  }

  def foldLeft[S](init: S)(f: (S, (UserColumnId, V)) => S): S =  {
    var seed = init
    val it = unsafeUnderlying.entrySet.iterator
    while(it.hasNext) {
      val ent = it.next()
      seed = f(seed, (new UserColumnId(ent.getKey), ent.getValue))
    }
    seed
  }

  override def toString = unsafeUnderlying.toString

  def toSeq: Seq[(UserColumnId, V)] = {
    val arr = new Array[(UserColumnId, V)](unsafeUnderlying.size)
    val it = unsafeUnderlying.entrySet.iterator
    var i = 0
    while(it.hasNext) {
      val ent = it.next()
      arr(i) = (new UserColumnId(ent.getKey), ent.getValue)
      i += 1
    }
    arr
  }

  def foreach[U](f: ((UserColumnId, V)) => U) {
    val it = unsafeUnderlying.entrySet.iterator
    while(it.hasNext) {
      val ent = it.next()
      f((new UserColumnId(ent.getKey), ent.getValue))
    }
  }

  def foreach[U](f: (UserColumnId, V) => U) {
    val it = unsafeUnderlying.entrySet.iterator
    while(it.hasNext) {
      val ent = it.next()
      f(new UserColumnId(ent.getKey), ent.getValue)
    }
  }

  def filter(f: (UserColumnId, V) => Boolean) = {
    val x = new java.util.HashMap[String, V]
    val it = unsafeUnderlying.entrySet.iterator
    while(it.hasNext) {
      val ent = it.next()
      if(f(new UserColumnId(ent.getKey), ent.getValue)) x.put(ent.getKey, ent.getValue)
    }
    new UserColumnIdMap[V](x)
  }

  def filterNot(f: (UserColumnId, V) => Boolean) = {
    val x = new java.util.HashMap[String, V]
    val it = unsafeUnderlying.entrySet.iterator
    while(it.hasNext) {
      val ent = it.next()
      if(!f(new UserColumnId(ent.getKey), ent.getValue)) x.put(ent.getKey, ent.getValue)
    }
    new UserColumnIdMap[V](x)
  }

  override def hashCode = unsafeUnderlying.hashCode
  override def equals(o: Any) = o match {
    case that: UserColumnIdMap[_] => this.unsafeUnderlying == that.unsafeUnderlying
    case _ => false
  }
}
