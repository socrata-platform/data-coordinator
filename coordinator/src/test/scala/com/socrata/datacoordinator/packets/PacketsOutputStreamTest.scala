package com.socrata.datacoordinator.packets

import java.io.{ByteArrayOutputStream, InputStream}

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import org.scalatest.prop.PropertyChecks
import org.scalacheck.{Arbitrary, Gen}

class PacketsOutputStreamTest extends FunSuite with MustMatchers with PropertyChecks {
  val minSize = PacketsOutputStream.minimumSizeFor(PacketsStream.defaultDataLabel, PacketsStream.defaultEndLabel)
  val maxSize = 100000

  test("Simply closing a PacketsOutputStream produces just an end packet") {
    forAll(Gen.choose(minSize, Int.MaxValue)) { packetSize =>
      whenever(packetSize >= minSize) {
        val sink = new PacketsSink(packetSize)
        val pos = new PacketsOutputStream(sink)
        pos.close()
        sink.results must equal (List(new PacketsStream.EndPacket()()))
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
    forAll(Gen.choose(minSize, maxSize), Arbitrary.arbitrary[List[List[List[Byte]]]]) { (packetSize, dataSeq) =>
      val data = dataSeq.map(_.flatten.toArray)
      whenever(packetSize >= minSize && packetSize <= maxSize && data.map(_.length).sum > packetSize) {
        val sink = new PacketsSink(packetSize)
        val pos = new PacketsOutputStream(sink)
        data.foreach(pos.write)
        pos.flush()
        sink.results.dropRight(1) /* The last one might not be full */.foreach { b =>
          b.buffer.remaining must equal (packetSize)
        }
      }
    }
  }

  test("Writing data produces that same data") {
    forAll(Gen.choose(minSize, maxSize), Arbitrary.arbitrary[List[List[List[Byte]]]]) { (packetSize, dataSeq) =>
      val data = dataSeq.map(_.flatten.toArray)
      whenever(packetSize >= minSize && packetSize <= maxSize) {
        val sink = new PacketsSink(packetSize)
        val pos = new PacketsOutputStream(sink)
        data.foreach(pos.write)
        pos.close()
        readAll(new PacketsInputStream(new PacketsReservoir(sink.results : _*))) must equal (data.toArray.flatten)
      }
    }
  }
}
