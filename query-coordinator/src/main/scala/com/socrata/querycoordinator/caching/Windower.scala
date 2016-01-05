package com.socrata.querycoordinator.caching

case class WindowPos(window: BigInt, index: Int)

/* A function which takes a limit and offset, and transforms them into a pair
 * of Windows containing the start and end.
 */
class Windower(val windowSize: Int) extends ((BigInt, BigInt) => (WindowPos, WindowPos)) {
  require(windowSize > 0, "Window size must be positive")

  def apply(limit: BigInt, offset: BigInt) = {
    val startWindow = WindowPos(offset / windowSize, (offset % windowSize).toInt)
    val endWindow = locally {
      val itemAfterLast = offset + limit
      val windowOfItemAfterLast = itemAfterLast / windowSize
      if(windowSize * windowOfItemAfterLast == itemAfterLast) WindowPos(windowOfItemAfterLast - 1, windowSize)
      else WindowPos(windowOfItemAfterLast, (itemAfterLast % windowSize).toInt)
    }
    (startWindow, endWindow)
  }
}
