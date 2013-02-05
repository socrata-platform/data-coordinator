package com.socrata.datacoordinator.packets

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

private[packets] class UncopyingByteArrayOutputStream extends ByteArrayOutputStream {
  def underlyingByteArray = buf

  def buffer = ByteBuffer.wrap(buf, 0, count)
  def clear() {
    buf = new Array[Byte](32)
    count = 0
  }
}
