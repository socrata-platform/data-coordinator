package com.socrata.datacoordinator
package util

import scala.collection.JavaConverters._

import gnu.trove.map.hash.TLongObjectHashMap
import gnu.trove.iterator.TLongObjectIterator
import gnu.trove.set.TLongSet
import gnu.trove.set.hash.TLongHashSet
import gnu.trove.procedure.TLongProcedure

object LongLikeMap {
  private def copyToTMap[T <: Long, V](m: Map[T, V]) = {
    val result = new TLongObjectHashMap[V]
    for((k, v) <- m) result.put(k, v)
    result
  }

  def apply[T <: Long, V](kvs: (T, V)*) = {
    val tmp = new TLongObjectHashMap[V]()
    for((t, v) <- kvs) {
      tmp.put(t, v)
    }
    new LongLikeMap[T, V](tmp)
  }
}

class LongLikeMap[T <: Long, V](val underlying: TLongObjectHashMap[V]) /* extends AnyVal -- oh man I so want value classes! */ {
  def this() = this(new TLongObjectHashMap[V])
  def this(orig: LongLikeMap[T, V]) = this(new TLongObjectHashMap(orig.underlying))
  def this(orig: Map[T, V]) = this(LongLikeMap.copyToTMap(orig))

  @inline def size = underlying.size

  @inline def isEmpty = underlying.isEmpty

  @inline def nonEmpty = !underlying.isEmpty

  @inline def contains(t: T) = underlying.contains(t)

  @inline def get(t: T) = {
    val x = underlying.get(t)
    if(x.asInstanceOf[AnyRef] eq null) None
    else Some(x)
  }

  @inline def apply(t: T) = {
    val x = underlying.get(t)
    if(x.asInstanceOf[AnyRef] eq null) throw new NoSuchElementException("No key " + t)
    x
  }

  def iterator = new LongLikeMapIterator[T, V](underlying.iterator)

  def ++(that: LongLikeMap[T, V]) = {
    val tmp = new TLongObjectHashMap[V]
    tmp.putAll(this.underlying)
    tmp.putAll(that.underlying)
    new LongLikeMap[T, V](tmp)
  }

  @inline def +=(kv: (T, V)) {
    update(kv._1, kv._2)
  }

  @inline def -=(k: T) {
    underlying.remove(k)
  }

  @inline def update(k: T, v: V) {
    if(v.asInstanceOf[AnyRef] eq null) throw new NullPointerException("Cannot store null values here")
    underlying.put(k, v)
  }

  @inline def getOrElse[B >: V](k: T, v: => B): B = {
    val result = underlying.get(k)
    if(result == null) v
    else result
  }

  def keys: Iterator[T] = iterator.map(_._1)
  def values: Iterable[V] = underlying.valueCollection.asScala

  def keySet = new LongLikeSet[T](underlying.keySet)

  def mapValuesStrict[V2](f: V => V2) = {
    val x = new TLongObjectHashMap[V2]
    val it = underlying.iterator
    while(it.hasNext) {
      it.advance()
      x.put(it.key, f(it.value))
    }
    new LongLikeMap[T, V2](x)
  }

  def transform[V2](f: (T, V) => V2) = {
    val x = new TLongObjectHashMap[V2]
    val it = underlying.iterator
    while(it.hasNext) {
      it.advance()
      x.put(it.key, f(it.key.asInstanceOf[T], it.value))
    }
    new LongLikeMap[T, V2](x)
  }

  def foldLeft[S](init: S)(f: (S, (T, V)) => S): S =  {
    var seed = init
    val it = underlying.iterator
    while(it.hasNext) {
      it.advance()
      seed = f(seed, (it.key.asInstanceOf[T], it.value))
    }
    seed
  }

  override def toString = underlying.toString

  def toSeq = iterator.toSeq

  override def hashCode = underlying.hashCode
  override def equals(o: Any) = o match {
    case that: LongLikeMap[_, _] => this.underlying == that.underlying
    case _ => false
  }
}

class LongLikeSet[T <: Long](val underlying: TLongSet) extends (T => Boolean) {
  def apply(x: T) = underlying.contains(x)
  def filter(f: T => Boolean) = {
    val result = new TLongHashSet
    underlying.forEach(new TLongProcedure {
      def execute(l: Long) = {
        if(f(l.asInstanceOf[T])) result.add(l)
        true
      }
    })
    new LongLikeSet[T](result)
  }
  def toSet = {
    val b = Set.newBuilder[T]
    underlying.forEach(new TLongProcedure {
      def execute(l: Long) = {
        b += l.asInstanceOf[T]
        true
      }
    })
    b.result()
  }
}

class LongLikeMapIterator[T <: Long, V](val underlying: TLongObjectIterator[V]) extends Iterator[(T, V)] {
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
