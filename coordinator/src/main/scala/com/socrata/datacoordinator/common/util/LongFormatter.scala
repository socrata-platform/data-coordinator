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

  def deformat(s: String): Option[Long] =
    try { Some(deformatEx(s)) }
    catch { case e: IllegalArgumentException => None }

  def deformatEx(s: String): Long = {
    if(s.length != 14) badRid()
    val q1 = dequadify(s, 0).toLong
    val p1 = depunctEx(s.charAt(4)).toLong
    val q2 = dequadify(s, 5).toLong
    val p2 = depunctEx(s.charAt(9)).toLong
    val q3 = dequadify(s, 10).toLong
    q1 + (q2 << 20) + (q3 << 40) + (p1 << 60) + (p2 << 62)
  }

  def dequadify(s: String, offset: Int) =
    try { quadifier.dequadifyEx(s, offset) }
    catch { case _: IllegalArgumentException => badRid() }

  private def badRid() =
    throw new IllegalArgumentException(badRidMessage)
}
