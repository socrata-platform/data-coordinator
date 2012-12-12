package com.socrata.datacoordinator.util.collection

import gnu.trove.set.TLongSet
import gnu.trove.set.hash.TLongHashSet
import gnu.trove.procedure.TLongProcedure

class LongLikeSet[T <: Long](val unsafeUnderlying: TLongSet) extends (T => Boolean) {
  def apply(x: T) = unsafeUnderlying.contains(x)

  def filter(f: T => Boolean) = {
    val result = new TLongHashSet
    unsafeUnderlying.forEach(new TLongProcedure {
      def execute(l: Long) = {
        if(f(l.asInstanceOf[T])) result.add(l)
        true
      }
    })
    new LongLikeSet[T](result)
  }

  def toSet = {
    val b = Set.newBuilder[T]
    unsafeUnderlying.forEach(new TLongProcedure {
      def execute(l: Long) = {
        b += l.asInstanceOf[T]
        true
      }
    })
    b.result()
  }
}

