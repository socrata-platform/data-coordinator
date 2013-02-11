package com.socrata.datacoordinator.packets

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import org.scalatest.prop.PropertyChecks
import org.scalacheck.{Arbitrary, Gen}
import java.io.{ByteArrayOutputStream, InputStream}

class PacketsOutputStreamTest extends FunSuite with MustMatchers with PropertyChecks {
  test("Simply closing a PacketsOutputStream produces just an end packet") {
    forAll { packetSize: Int =>
      whenever(packetSize >= PacketsOutputStream.minimumSize) {
        val sink = new PacketsSink(packetSize)
        val pos = new PacketsOutputStream(sink)
        pos.close()
        sink.results must equal (List(PacketsStream.End()))
      }
    }
  }

  def readAll(in: InputStream): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val buf = new Array[Byte](10000)
    def loop() {
      in.read(buf) match {
        case -1 => // done
        case n => baos.write(buf, 0, n); loop()
      }
    }
    loop()
    baos.toByteArray
  }

  test("Writing data produces uniformly-sized data") {
    val max = 100000
    forAll(Gen.choose(0, max), Arbitrary.arbitrary[List[Array[Byte]]]) { (packetSize: Int, data: List[Array[Byte]]) =>
      whenever(packetSize >= PacketsOutputStream.minimumSize && packetSize <= max) {
        val sink = new PacketsSink(packetSize)
        val pos = new PacketsOutputStream(sink)
        data.foreach(pos.write)
        pos.flush()
        sink.results.dropRight(1).forall(_.buffer.remaining == packetSize)
      }
    }
  }

  test("Writing data produces that same data") {
    val max = 100000
    forAll(Gen.choose(0, max), Arbitrary.arbitrary[List[Array[Byte]]]) { (packetSize: Int, data: List[Array[Byte]]) =>
      whenever(packetSize >= PacketsOutputStream.minimumSize && packetSize <= max) {
        val sink = new PacketsSink(packetSize)
        val pos = new PacketsOutputStream(sink)
        data.foreach(pos.write)
        pos.close()
        readAll(new PacketsInputStream(new PacketsReservoir(sink.results : _*))) must equal (data.toArray.flatten)
      }
    }
  }
}
