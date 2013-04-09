package com.socrata.datacoordinator.util

class Counter(init: Int = 0) {
  private var next = init

  def apply() = {
    val r = next
    next += 1
    r
  }

  def lastValue = next - 1

  def peek = next
}
