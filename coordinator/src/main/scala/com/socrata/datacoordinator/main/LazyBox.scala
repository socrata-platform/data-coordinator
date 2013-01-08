package com.socrata.datacoordinator.main

class LazyBox[T](x: => T) {
  private var evaluated = false
  private lazy val v = x

  def initialized = evaluated

  def value = {
    if(!evaluated) {
      val result = v
      evaluated = true
      result
    } else {
      v
    }
  }
}
