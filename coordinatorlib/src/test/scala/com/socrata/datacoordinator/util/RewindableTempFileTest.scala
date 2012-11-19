package com.socrata.datacoordinator.util

import java.io.DataInputStream

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import org.scalatest.prop.PropertyChecks

import com.rojoma.simplearm.util._
import org.scalacheck.{Gen, Arbitrary}

class RewindableTempFileTest extends FunSuite with MustMatchers with PropertyChecks {
  test("Should be readable after writing") {
    forAll(implicitly[Arbitrary[Array[Array[Byte]]]].arbitrary, Gen.choose(1, 1000), Gen.choose(1, 1000)) { (bss, readBuf, writeBuf) =>
      whenever(readBuf > 0 && writeBuf > 0) {
        using(new RewindableTempFile(readBlockSize = readBuf, writeBlockSize = writeBuf)) { rtf =>
          for(bs <- bss) rtf.outputStream.write(bs)

          val buf = new Array[Byte](bss.iterator.map(_.length).sum)
          new DataInputStream(rtf.inputStream).readFully(buf)

          buf must equal (bss.flatten)
        }
      }
    }
  }
}
