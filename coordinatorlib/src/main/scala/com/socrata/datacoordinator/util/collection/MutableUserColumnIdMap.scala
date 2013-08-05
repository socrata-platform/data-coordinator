package com.socrata.datacoordinator.util.collection

import scala.collection.JavaConverters._
import com.socrata.datacoordinator.id.UserColumnId

object MutableUserColumnIdMap {
  private def copyToTMap[V](m: Map[UserColumnId, V]) = {
    val result = new java.util.HashMap[String, V]
    for((k, v) <- m) {
      if(v.asInstanceOf[AnyRef] eq null) throw new NullPointerException("Cannot store null values here")
      result.put(k.underlying, v)
    }
    result
  }

  def apply[V](kvs: (UserColumnId, V)*): MutableUserColumnIdMap[V] = {
    val result = new MutableUserColumnIdMap[V]
    result ++= kvs
    result
  }

  def apply[V](kvs: UserColumnIdMap[V]): MutableUserColumnIdMap[V] = {
    val result = new MutableUserColumnIdMap[V]
    result ++= kvs
    result
  }

  def apply[V](orig: Map[UserColumnId, V]): MutableUserColumnIdMap[V] =
  new MutableUserColumnIdMap(copyToTMap(orig))
}

class MutableUserColumnIdMap[V](private var _underlying: java.util.HashMap[String, V]) {
  def this() = this(new java.util.HashMap[String, V])
  def this(orig: MutableUserColumnIdMap[V]) = this(new java.util.HashMap[String, V](orig.underlying))
  def this(orig: UserColumnIdMap[V]) = this(new java.util.HashMap[String, V](orig.unsafeUnderlying))

  def underlying = _underlying

  def find(f: (UserColumnId, V) => Boolean): Option[(UserColumnId, V)] = {
    val it = _underlying.entrySet.iterator
    while(it.hasNext) {
      val ent = it.next()
      val cid = new UserColumnId(ent.getKey)
      if(f(cid, ent.getValue)) return Some(cid -> ent.getValue)
    }
    None
  }

  def freeze() = {
    if(underlying == null) throw new NullPointerException
    val result = new UserColumnIdMap[V](underlying)
    _underlying = null
    result
  }

  def frozenCopy() = {
    if(underlying == null) throw new NullPointerException
    val tmp = new java.util.HashMap[String, V](_underlying)
    new UserColumnIdMap[V](tmp)
  }

  @inline def size = underlying.size

  @inline def isEmpty = underlying.isEmpty

  @inline def nonEmpty = !underlying.isEmpty

  @inline def contains(t: UserColumnId) = underlying.containsKey(t.underlying)

  @inline def get(t: UserColumnId) = {
    val x = underlying.get(t.underlying)
    if(x.asInstanceOf[AnyRef] eq null) None
    else Some(x)
  }

  @inline def apply(t: UserColumnId) = {
    val x = underlying.get(t.underlying)
    if(x.asInstanceOf[AnyRef] eq null) throw new NoSuchElementException("No key " + t)
    x
  }

  def iterator = new UserColumnIdMapIterator[V](underlying.entrySet.iterator)

  def ++(that: MutableUserColumnIdMap[V]) = {
    val tmp = new java.util.HashMap[String, V]
    tmp.putAll(this.underlying)
    tmp.putAll(that.underlying)
    new UserColumnIdMap[V](tmp)
  }

  def ++(that: UserColumnIdMap[V]) = {
    val tmp = new java.util.HashMap[String, V]
    tmp.putAll(this.underlying)
    tmp.putAll(that.unsafeUnderlying)
    new UserColumnIdMap[V](tmp)
  }

  def ++(that: Iterable[(UserColumnId, V)]) = {
    val tmp = new java.util.HashMap[String, V]
    tmp.putAll(this.underlying)
    for((k, v) <- that) {
      tmp.put(k.underlying, v)
    }
    new UserColumnIdMap[V](tmp)
  }

  def ++=(that: MutableUserColumnIdMap[V]) {
    this.underlying.putAll(that.underlying)
  }

  def ++=(that: UserColumnIdMap[V]) {
    this.underlying.putAll(that.unsafeUnderlying)
  }

  def ++=(that: Iterable[(UserColumnId, V)]) {
    for(kv <- that) {
      this += kv
    }
  }

  @inline def +=(kv: (UserColumnId, V)) {
    update(kv._1, kv._2)
  }

  @inline def -=(k: UserColumnId) {
    underlying.remove(k.underlying)
  }

  @inline def update(k: UserColumnId, v: V) {
    if(v.asInstanceOf[AnyRef] eq null) throw new NullPointerException("Cannot store null values here")
    underlying.put(k.underlying, v)
  }

  @inline def getOrElse[B >: V](k: UserColumnId, v: => B): B = {
    val result = underlying.get(k.underlying)
    if(result == null) v
    else result
  }

  def keys: Iterator[UserColumnId] = iterator.map(_._1)
  def values: Iterable[V] = underlying.values.asScala

  def keySet = new UserColumnIdSet(underlying.keySet)

  def mapValuesStrict[V2](f: V => V2) = {
    val x = new java.util.HashMap[String, V2]
    val it = underlying.entrySet.iterator
    while(it.hasNext) {
      val ent = it.next()
      x.put(ent.getKey, f(ent.getValue))
    }
    new UserColumnIdMap[V2](x)
  }

  def transform[V2](f: (UserColumnId, V) => V2) = {
    val x = new java.util.HashMap[String, V2]
    val it = underlying.entrySet.iterator
    while(it.hasNext) {
      val ent = it.next()
      x.put(ent.getKey, f(new UserColumnId(ent.getKey), ent.getValue))
    }
    new UserColumnIdMap[V2](x)
  }

  def foldLeft[S](init: S)(f: (S, (UserColumnId, V)) => S): S =  {
    var seed = init
    val it = underlying.entrySet.iterator
    while(it.hasNext) {
      val ent = it.next()
      seed = f(seed, (new UserColumnId(ent.getKey), ent.getValue))
    }
    seed
  }

  def clear() {
    underlying.clear()
  }

  override def toString = underlying.toString

  def toSeq = iterator.toSeq

  override def hashCode = underlying.hashCode
  override def equals(o: Any) = o match {
    case that: MutableUserColumnIdMap[_] => this.underlying == that.underlying
    case _ => false
  }
}
