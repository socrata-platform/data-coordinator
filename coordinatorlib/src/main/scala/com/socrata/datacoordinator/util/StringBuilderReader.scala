package com.socrata.datacoordinator.util

import java.io.Reader

class StringBuilderReader(sb: java.lang.StringBuilder) extends Reader {
  private var srcPtr = 0

  def read(dst: Array[Char], off: Int, len: Int): Int = {
    val remaining = sb.length - srcPtr
    if(remaining == 0) return -1
    val count = java.lang.Math.min(remaining, len)
    val end = srcPtr + count
    sb.getChars(srcPtr, end, dst, off)
    srcPtr = end
    count
  }

  override def read(): Int = {
    val remaining = sb.length - srcPtr
    if(remaining == 0) return -1
    val result = sb.charAt(srcPtr)
    srcPtr += 1
    result
  }

  override def skip(n: Long): Long = {
    if(n < 0) throw new IllegalArgumentException("skip is negative")

    val remaining = sb.length - srcPtr
    val count = java.lang.Math.min(remaining, n).toInt
    srcPtr += count
    count
  }

  def close() {}
}
