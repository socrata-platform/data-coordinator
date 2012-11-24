package com.socrata.datacoordinator.util

import scala.collection.mutable
import gnu.trove.map.hash.TIntObjectHashMap

// Adapted from the scala collections JavaConverters java.util.Map wrapper

trait TIntObjectHashMapWrapperLike[B, +Repr <: mutable.MapLike[Int, B, Repr] with mutable.Map[Int, B]]
  extends mutable.Map[Int, B] with mutable.MapLike[Int, B, Repr]
{
  def underlying: TIntObjectHashMap[B]

  override def size = underlying.size

  def get(k : Int) = {
    val v = underlying.get(k)
    if (v != null)
      Some(v)
    else if(underlying.containsKey(k))
      Some(null.asInstanceOf[B])
    else
      None
  }

  def +=(kv: (Int, B)): this.type = { underlying.put(kv._1, kv._2); this }
  def -=(key: Int): this.type = { underlying.remove(key); this }

  override def put(k : Int, v : B): Option[B] = {
    val r = underlying.put(k, v)
    if (r != null) Some(r) else None
  }

  override def update(k : Int, v : B) { underlying.put(k, v) }

  override def remove(k : Int): Option[B] = {
    val r = underlying.remove(k)
    if (r != null) Some(r) else None
  }

  def iterator = new Iterator[(Int, B)] {
    val ui = underlying.iterator
    def hasNext = ui.hasNext
    def next() = {
      ui.advance()
      (ui.key, ui.value)
    }
  }

  override def clear() = underlying.clear()

  override def empty: Repr = null.asInstanceOf[Repr]
}

case class TIntObjectHashMapWrapper[B](val underlying : TIntObjectHashMap[B])
  extends TIntObjectHashMapWrapperLike[B, TIntObjectHashMapWrapper[B]] {
  override def empty = TIntObjectHashMapWrapper(new TIntObjectHashMap[B])
}
