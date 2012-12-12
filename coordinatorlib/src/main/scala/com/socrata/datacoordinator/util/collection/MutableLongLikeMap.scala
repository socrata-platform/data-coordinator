package com.socrata.datacoordinator.util.collection

import scala.collection.JavaConverters._
import gnu.trove.map.hash.TLongObjectHashMap

object MutableLongLikeMap {
  private def copyToTMap[T <: Long, V](m: Map[T, V]) = {
    val result = new TLongObjectHashMap[V]
    for((k, v) <- m) result.put(k, v)
    result
  }
}

class MutableLongLikeMap[K <: Long, V](private var _underlying: TLongObjectHashMap[V]) {
  def this() = this(new TLongObjectHashMap[V])
  def this(orig: LongLikeMap[K, V]) = this(new TLongObjectHashMap(orig.unsafeUnderlying))
  def this(orig: MutableLongLikeMap[K, V]) = this(new TLongObjectHashMap(orig.underlying))
  def this(orig: Map[K, V]) = this(MutableLongLikeMap.copyToTMap(orig))

  def underlying = _underlying

  def freeze() = {
    if(underlying == null) throw new NullPointerException
    val result = new LongLikeMap[K, V](underlying)
    _underlying = null
    result
  }

  @inline def size = underlying.size

  @inline def isEmpty = underlying.isEmpty

  @inline def nonEmpty = !underlying.isEmpty

  @inline def contains(t: K) = underlying.contains(t)

  @inline def get(t: K) = {
    val x = underlying.get(t)
    if(x.asInstanceOf[AnyRef] eq null) None
    else Some(x)
  }

  @inline def apply(t: K) = {
    val x = underlying.get(t)
    if(x.asInstanceOf[AnyRef] eq null) throw new NoSuchElementException("No key " + t)
    x
  }

  def iterator = new LongLikeMapIterator[K, V](underlying.iterator)

  def ++(that: MutableLongLikeMap[K, V]) = {
    val tmp = new TLongObjectHashMap[V]
    tmp.putAll(this.underlying)
    tmp.putAll(that.underlying)
    new LongLikeMap[K, V](tmp)
  }

  def ++(that: LongLikeMap[K, V]) = {
    val tmp = new TLongObjectHashMap[V]
    tmp.putAll(this.underlying)
    tmp.putAll(that.unsafeUnderlying)
    new LongLikeMap[K, V](tmp)
  }

  def ++(that: Iterable[(K, V)]) = {
    val tmp = new TLongObjectHashMap[V]
    tmp.putAll(this.underlying)
    for((k, v) <- that) {
      tmp.put(k, v)
    }
    new LongLikeMap[K, V](tmp)
  }

  def ++=(that: MutableLongLikeMap[K, V]) {
    this.underlying.putAll(that.underlying)
  }

  def ++=(that: LongLikeMap[K, V]) {
    this.underlying.putAll(that.unsafeUnderlying)
  }

  def ++=(that: Iterable[(K, V)]) {
    for(kv <- that) {
      this += kv
    }
  }

  @inline def +=(kv: (K, V)) {
    update(kv._1, kv._2)
  }

  @inline def -=(k: K) {
    underlying.remove(k)
  }

  @inline def update(k: K, v: V) {
    if(v.asInstanceOf[AnyRef] eq null) throw new NullPointerException("Cannot store null values here")
    underlying.put(k, v)
  }

  @inline def getOrElse[B >: V](k: K, v: => B): B = {
    val result = underlying.get(k)
    if(result == null) v
    else result
  }

  def keys: Iterator[K] = iterator.map(_._1)
  def values: Iterable[V] = underlying.valueCollection.asScala

  def keySet = new LongLikeSet[K](underlying.keySet)

  def mapValuesStrict[V2](f: V => V2) = {
    val x = new TLongObjectHashMap[V2]
    val it = underlying.iterator
    while(it.hasNext) {
      it.advance()
      x.put(it.key, f(it.value))
    }
    new LongLikeMap[K, V2](x)
  }

  def transform[V2](f: (K, V) => V2) = {
    val x = new TLongObjectHashMap[V2]
    val it = underlying.iterator
    while(it.hasNext) {
      it.advance()
      x.put(it.key, f(it.key.asInstanceOf[K], it.value))
    }
    new LongLikeMap[K, V2](x)
  }

  def foldLeft[S](init: S)(f: (S, (K, V)) => S): S =  {
    var seed = init
    val it = underlying.iterator
    while(it.hasNext) {
      it.advance()
      seed = f(seed, (it.key.asInstanceOf[K], it.value))
    }
    seed
  }

  override def toString = underlying.toString

  def toSeq = iterator.toSeq

  override def hashCode = underlying.hashCode
  override def equals(o: Any) = o match {
    case that: MutableLongLikeMap[_, _] => this.underlying == that.underlying
    case _ => false
  }
}
