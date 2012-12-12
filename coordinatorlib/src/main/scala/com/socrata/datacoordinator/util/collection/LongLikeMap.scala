package com.socrata.datacoordinator.util.collection

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.JavaConverters._

import gnu.trove.map.hash.TLongObjectHashMap
import gnu.trove.iterator.TLongObjectIterator

object LongLikeMap {
  def apply[K <: Long, V](kvs: (K, V)*) = {
    val tmp = new MutableLongLikeMap[K, V]
    tmp ++= kvs
    tmp.freeze()
  }
}

class LongLikeMap[K <: Long, +V] private[collection] (val unsafeUnderlying: TLongObjectHashMap[V @uncheckedVariance]) /* extends AnyVal -- oh man I so want value classes! */ {
  def this(orig: Map[K, V]) = this(new MutableLongLikeMap(orig).underlying)

  @inline def size = unsafeUnderlying.size

  @inline def isEmpty = unsafeUnderlying.isEmpty

  @inline def nonEmpty = !isEmpty

  @inline def contains(t: K) = unsafeUnderlying.contains(t)

  @inline def get(t: K) = {
    val x = unsafeUnderlying.get(t)
    if(x.asInstanceOf[AnyRef] eq null) None
    else Some(x)
  }

  @inline def apply(t: K) = {
    val x = unsafeUnderlying.get(t)
    if(x.asInstanceOf[AnyRef] eq null) throw new NoSuchElementException("No key " + t)
    x
  }

  def iterator = new LongLikeMapIterator[K, V](unsafeUnderlying.iterator)

  def ++[V2 >: V](that: LongLikeMap[K, V2]) = {
    val tmp = new TLongObjectHashMap[V2](this.unsafeUnderlying)
    tmp.putAll(that.unsafeUnderlying)
    new LongLikeMap[K, V2](tmp)
  }

  def ++[V2 >: V](that: MutableLongLikeMap[K, V2]) = {
    val tmp = new TLongObjectHashMap[V2](this.unsafeUnderlying)
    tmp.putAll(that.underlying)
    new LongLikeMap[K, V2](tmp)
  }

  def ++[V2 >: V](that: Iterable[(K, V2)]) = {
    val tmp = new TLongObjectHashMap[V2](this.unsafeUnderlying)
    for((k, v) <- that) {
      tmp.put(k, v)
    }
    new LongLikeMap[K, V2](tmp)
  }

  @inline def getOrElse[B >: V](k: K, v: => B): B = {
    val result = unsafeUnderlying.get(k)
    if(result == null) v
    else result
  }

  def keys: Iterator[K] = iterator.map(_._1)
  def values: Iterable[V] = unsafeUnderlying.valueCollection.asScala

  def keySet = new LongLikeSet[K](unsafeUnderlying.keySet)

  def mapValuesStrict[V2](f: V => V2) = {
    val x = new TLongObjectHashMap[V2]
    val it = unsafeUnderlying.iterator
    while(it.hasNext) {
      it.advance()
      x.put(it.key, f(it.value))
    }
    new LongLikeMap[K, V2](x)
  }

  def transform[V2](f: (K, V) => V2) = {
    val x = new TLongObjectHashMap[V2]
    val it = unsafeUnderlying.iterator
    while(it.hasNext) {
      it.advance()
      x.put(it.key, f(it.key.asInstanceOf[K], it.value))
    }
    new LongLikeMap[K, V2](x)
  }

  def foldLeft[S](init: S)(f: (S, (K, V)) => S): S =  {
    var seed = init
    val it = unsafeUnderlying.iterator
    while(it.hasNext) {
      it.advance()
      seed = f(seed, (it.key.asInstanceOf[K], it.value))
    }
    seed
  }

  override def toString = unsafeUnderlying.toString

  def toSeq: Seq[(K, V)] = {
    val arr = new Array[(K, V)](unsafeUnderlying.size)
    val it = unsafeUnderlying.iterator
    var i = 0
    while(it.hasNext) {
      it.advance()
      arr(i) = (it.key.asInstanceOf[K], it.value)
      i += 1
    }
    arr
  }

  override def hashCode = unsafeUnderlying.hashCode
  override def equals(o: Any) = o match {
    case that: LongLikeMap[_, _] => this.unsafeUnderlying == that.unsafeUnderlying
    case _ => false
  }
}

class LongLikeMapIterator[T <: Long, +V](val underlying: TLongObjectIterator[V @uncheckedVariance]) extends Iterator[(T, V)] {
  def hasNext = underlying.hasNext
  def next() = {
    advance()
    (underlying.key.asInstanceOf[T], underlying.value)
  }
  def advance() {
    underlying.advance()
  }
  def key = underlying.key.asInstanceOf[T]
  def value = underlying.value
}
