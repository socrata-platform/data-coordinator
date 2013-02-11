package com.socrata.datacoordinator.packets

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import org.scalatest.prop.PropertyChecks
import java.io.{ByteArrayOutputStream, InputStream}

class PacketsInputStreamTest extends FunSuite with MustMatchers with PropertyChecks {
  def toDataPacket(xs: Array[Byte]) = {
    val pos = Packet.labelledPacketStream(PacketsStream.streamLabel)
    pos.write(xs)
    pos.packet()
  }

  def readAll(in: InputStream) = {
    val baos = new ByteArrayOutputStream()
    val buf = new Array[Byte](1024)
    def loop() {
      in.read(buf) match {
        case -1 => // done
        case n => baos.write(buf, 0, n); loop()
      }
    }
    loop()
    baos.toByteArray
  }

  test("Can read sent data") {
    forAll { xs: List[Array[Byte]] =>
      val reservoir = new PacketsReservoir(xs.map(toDataPacket) :+ PacketsStream.End() : _*)
      readAll(new PacketsInputStream(reservoir)) must equal (xs.toArray.flatten)
    }
  }
}
