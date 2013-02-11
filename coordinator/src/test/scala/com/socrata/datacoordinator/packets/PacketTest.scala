package com.socrata.datacoordinator.packets

import org.scalatest.FunSuite
import org.scalatest.prop.PropertyChecks
import org.scalatest.matchers.MustMatchers
import java.nio.ByteBuffer

class PacketTest extends FunSuite with PropertyChecks with MustMatchers {
  test("Packet's constructor rejects a packet with an incorrect length") {
    forAll { (xs: Array[Byte], i: Int) =>
      whenever(i != xs.length + 4) {
        val buf = ByteBuffer.allocate(xs.length + 4)
        buf.putInt(i)
        buf.put(xs)
        buf.flip()
        evaluating { new Packet(buf) } must produce [IllegalArgumentException]
      }
    }
  }

  test("Packet's constructor rejects a packet with fewer than four bytes") {
    for(i <- 0 until 4) {
      val buf = ByteBuffer.allocate(i)
      evaluating { new Packet(buf) } must produce [IllegalArgumentException]
    }
  }

  test("Packet's contructor accepts a packet with the right length") {
    forAll { xs: Array[Byte] =>
      val buf = ByteBuffer.allocate(xs.length + 4)
      buf.putInt(buf.remaining)
      buf.put(xs)
      buf.flip()
      new Packet(buf) : Unit
    }
  }

  test("Can get data out of a packet") {
    forAll { xs: Array[Byte] =>
      val buf = ByteBuffer.allocate(xs.length + 4)
      buf.putInt(buf.remaining)
      buf.put(xs)
      buf.flip()
      new Packet(buf).data must equal (ByteBuffer.wrap(xs))
    }
  }

  test("Can get the length of data out of a packet") {
    forAll { xs: Array[Byte] =>
      val buf = ByteBuffer.allocate(xs.length + 4)
      buf.putInt(buf.remaining)
      buf.put(xs)
      buf.flip()
      new Packet(buf).dataSize must equal (xs.length)
    }
  }
}
