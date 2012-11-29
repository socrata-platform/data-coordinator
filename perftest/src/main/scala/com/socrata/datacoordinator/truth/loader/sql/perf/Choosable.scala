package com.socrata.datacoordinator
package truth.loader
package sql
package perf

import scala.collection.mutable
import scala.util.Random

class Choosable[T](rng: Random) {
  private var xs = new Array[AnyRef](0)
  private val additions = new mutable.HashSet[T]
  private val set = new mutable.HashSet[T]

  def isEmpty = set.isEmpty

  private def refresh() {
    additions.clear()
    xs = new Array[AnyRef](set.size)
    var i = 0
    for(v <- set) {
      xs(i) = v.asInstanceOf[AnyRef]
      i += 1
    }
  }

  def +=(t: T) = {
    if(!set.contains(t)) {
      set += t
      additions += t
    }
  }

  def pick(): T = {
    if(additions.size > 10000) refresh()
    var i = 0
    while(i < 10) {
      val idx = rng.nextInt(xs.length + additions.size)
      if(idx >= xs.length) {
        return additions.iterator.drop(idx - xs.length).next()
      } else {
        val v = xs(idx)
        if(v != null) {
          return v.asInstanceOf[T]
        }
      }
      i += 1
    }
    refresh()
    pick()
  }

  def extract(): T = {
    if(additions.size > 10000) refresh()
    var i = 0
    while(i < 10) {
      val idx = rng.nextInt(xs.length + additions.size)
      if(idx >= xs.length) {
        val v = additions.iterator.drop(idx - xs.length).next()
        additions -= v
        set -= v
        return v
      } else {
        val v = xs(idx)
        if(v != null) {
          xs(idx) = null
          set -= v.asInstanceOf[T]
          return v.asInstanceOf[T]
        }
      }
      i += 1
    }
    refresh()
    extract()
  }
}
