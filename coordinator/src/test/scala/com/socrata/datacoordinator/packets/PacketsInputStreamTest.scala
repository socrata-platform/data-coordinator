package com.socrata.datacoordinator.packets

import java.io.{ByteArrayOutputStream, InputStream}

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import org.scalatest.prop.PropertyChecks

class PacketsInputStreamTest extends FunSuite with MustMatchers with PropertyChecks {
  def toDataPacket(xs: Array[Byte]) = new PacketsStream.DataPacket()(_.write(xs))

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
      val reservoir = new PacketsReservoir(xs.map(toDataPacket) :+ new PacketsStream.EndPacket()() : _*)
      readAll(new PacketsInputStream(reservoir)) must equal (xs.toArray.flatten)
    }
  }
}
