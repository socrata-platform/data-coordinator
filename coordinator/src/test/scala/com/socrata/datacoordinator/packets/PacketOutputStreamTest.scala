package com.socrata.datacoordinator.packets

import java.nio.ByteBuffer

import org.scalatest.FunSuite
import org.scalatest.prop.PropertyChecks
import org.scalatest.matchers.MustMatchers

class PacketOutputStreamTest extends FunSuite with PropertyChecks with MustMatchers {
  def write(xs: List[Array[Byte]]) = {
    val pos = new PacketOutputStream
    for(x <- xs) pos.write(x)
    pos.packet()
  }

  test("Can write data into a PacketOutputStream") {
    forAll { xsSeq: List[List[Byte]] =>
      val xs = xsSeq.map(_.toArray)
      write(xs).data must equal (ByteBuffer.wrap(xs.toArray.flatten))
    }
  }

  test("The resulting has the appropriate size buffer") {
    forAll { xsSeq: List[List[Byte]] =>
      val xs = xsSeq.map(_.toArray)
      val packet = write(xs)
      write(xs).buffer.getInt must equal (xs.map(_.length).sum + 4)
      packet.buffer.getInt must equal (packet.buffer.remaining)
    }
  }

  test("The resulting packet has the correct size of the data") {
    forAll { xsSeq: List[List[Byte]] =>
      val xs = xsSeq.map(_.toArray)
      write(xs).dataSize must equal (xs.map(_.length).sum)
    }
  }
}
