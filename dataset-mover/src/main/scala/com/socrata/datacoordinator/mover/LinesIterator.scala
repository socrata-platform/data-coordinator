package com.socrata.datacoordinator.mover

import scala.annotation.tailrec

import java.io.BufferedReader

class LinesIterator(reader: BufferedReader, keepEndOfLine: Boolean = false) extends Iterator[String] with BufferedIterator[String] {
  private var pending: String = null
  private var done = false

  def hasNext = pending != null || advance()

  def head =
    if(hasNext) pending
    else Iterator.empty.next()

  def next(): String =
    if(hasNext) {
      val result = pending
      pending = null
      result
    } else {
      Iterator.empty.next()
    }

  private def advance(): Boolean =
    if(done) {
      false
    } else {
      pending =
        if(keepEndOfLine) readIncludingEOL()
        else reader.readLine()
      done = pending == null
      !done
    }

  private def readIncludingEOL(): String = {
    val sb = new StringBuilder

    @tailrec
    def loop(): Unit = {
      reader.read() match {
        case -1 =>
          // done
        case 10 => // \n
          sb.append('\n')
        case 13 => // \r
          reader.mark(1)
          reader.read() match {
            case 10 =>
              sb.append("\r\n")
            case _ =>
              sb.append('\r')
              reader.reset()
          }
        case other =>
          sb.append(other.toChar)
          loop()
      }
    }

    loop()

    if(sb.nonEmpty) {
      sb.toString()
    } else {
      null
    }
  }
}
