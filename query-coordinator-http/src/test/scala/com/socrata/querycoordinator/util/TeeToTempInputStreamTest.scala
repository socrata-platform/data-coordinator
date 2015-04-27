package com.socrata.querycoordinator.util

import org.scalatest.FunSuite
import org.scalatest.MustMatchers
import org.scalatest.prop.PropertyChecks
import java.io.{ByteArrayOutputStream, InputStream, ByteArrayInputStream}
import com.rojoma.simplearm.util._

class TeeToTempInputStreamTest extends FunSuite with MustMatchers with PropertyChecks {
  private def readAll(in: InputStream): Array[Byte] = {
    val baos = new ByteArrayOutputStream
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

  private def read(in: InputStream, blockSize: Int, blockCount: Int): Int = {
    val trueBlockSize = blockSize & 0xff
    val trueBlockCount = blockCount & 0xf
    val bs = new Array[Byte](trueBlockSize)
    (1 to trueBlockCount).map(_ => in.read(bs)).filter(_ >= 0).sum
  }

  test("restream returns a stream equivalent to the original stream") {
    forAll { (xsR: Array[Array[Byte]], blockSize: Int, blockCount: Int) => // Array[Array[Byte]] to make spill-to-disk more probable
      val xs = xsR.flatten
      val inputStream = new ByteArrayInputStream(xs)
      using(new TeeToTempInputStream(inputStream, inMemoryBufferSize = 128)) { ts =>
        val count = read(ts, blockSize, blockCount)
        using(ts.restream()) { rs =>
          readAll(rs) must equal (java.util.Arrays.copyOf(xs, count))
          readAll(inputStream) must equal (java.util.Arrays.copyOfRange(xs, count, xs.length))
        }
      }
    }
  }

  test("The restream remains valid even after the teeing stream is closed") {
    forAll { (xsR: Array[Array[Byte]], blockSize: Int, blockCount: Int) =>
      val xs = xsR.flatten
      val inputStream = new ByteArrayInputStream(xs)
      var count = 0
      using(using(new TeeToTempInputStream(inputStream, inMemoryBufferSize = 128)) { ts =>
        count = read(ts, blockSize, blockCount)
        ts.restream()
      }) { rs =>
        readAll(rs) must equal (java.util.Arrays.copyOf(xs, count))
        readAll(inputStream) must equal (java.util.Arrays.copyOfRange(xs, count, xs.length))
      }
    }
  }
}
