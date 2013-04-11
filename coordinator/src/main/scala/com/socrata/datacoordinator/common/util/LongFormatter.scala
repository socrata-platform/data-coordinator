package com.socrata.datacoordinator.common.util

object LongFormatter {
  private val quadifier = Quadifier
  private val badRidMessage = "Not a valid row identifier"

  private val punct = Array('-','.','_','~') // chosen for URL-safety
  private def depunctEx(c: Char): Int = c match {
    case '-' => 0
    case '.' => 1
    case '_' => 2
    case '~' => 3
    case _ => badRid()
  }

  def format(x: Long): String = {
    val cs = new Array[Char](14)
    quadifier.quadify(x.toInt, cs, 0)
    cs(4) = punct((x >> 60).toInt & 3)
    quadifier.quadify((x >> 20).toInt, cs, 5)
    cs(9) = punct((x >> 62).toInt & 3)
    quadifier.quadify((x >> 40).toInt, cs, 10)
    new String(cs)
  }

  def deformat(s: String, offset: Int = 0): Option[Long] =
    try { Some(deformatEx(s, offset)) }
    catch { case e: IllegalArgumentException => None }

  def deformatEx(s: String, offset: Int = 0): Long = {
    if(s.length != offset + 14) badRid()
    val q1 = dequadify(s, offset).toLong
    val p1 = depunctEx(s.charAt(offset + 4)).toLong
    val q2 = dequadify(s, offset + 5).toLong
    val p2 = depunctEx(s.charAt(offset + 9)).toLong
    val q3 = dequadify(s, offset + 10).toLong
    q1 + (q2 << 20) + (q3 << 40) + (p1 << 60) + (p2 << 62)
  }

  def dequadify(s: String, offset: Int) =
    try { quadifier.dequadifyEx(s, offset) }
    catch { case _: IllegalArgumentException => badRid() }

  private def badRid() =
    throw new IllegalArgumentException(badRidMessage)
}
