package com.socrata.datacoordinator.util

import org.scalatest.FunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.MustMatchers
import java.io.{InputStream, ByteArrayOutputStream}
import org.scalacheck.{Arbitrary, Gen}
import com.rojoma.simplearm.v2._

class IndexedTempFileTest extends FunSuite with MustMatchers with ScalaCheckPropertyChecks {
  val lowerBound = 0xf
  val upperBound = 0xfff
  val bound = Gen.choose(lowerBound, upperBound)

  def flattenToArray(xs: Seq[Array[Byte]]): Array[Byte] = {
    val baos = new ByteArrayOutputStream(xs.iterator.map(_.length).sum)
    xs.foreach(baos.write)
    baos.toByteArray
  }

  def readAllFrom(xs: InputStream): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val buf = new Array[Byte](1024)
    def loop() {
      xs.read(buf) match {
        case -1 => // done
        case n => baos.write(buf, 0, n); loop()
      }
    }
    loop()
    baos.toByteArray
  }

  def roundtripTest(shuffleWrites: Boolean, shuffleReads: Boolean) {
    forAll(Arbitrary.arbitrary[Long], Arbitrary.arbitrary[List[List[Array[Byte]]]], bound, bound) { (seed, streams, indexBufSize, dataBufSize) =>
      whenever(indexBufSize >= lowerBound && dataBufSize >= lowerBound && indexBufSize <= upperBound && dataBufSize <= upperBound ) {
        val rng = new scala.util.Random(seed)

        def writes = if(shuffleWrites) rng.shuffle(streams.zipWithIndex) else streams.zipWithIndex
        def reads = if(shuffleReads) rng.shuffle(streams.zipWithIndex) else streams.zipWithIndex

        using(new IndexedTempFile(indexBufSizeHint = indexBufSize, dataBufSizeHint = dataBufSize)) { file =>
          writes.foreach { case (stream, idx) =>
            val s = file.newRecord(idx)
            for(chunk <- stream) s.write(chunk)
          }

          reads.foreach { case (stream, idx) =>
            val allBytes = flattenToArray(stream)
            val s = file.readRecord(idx).getOrElse {
              fail("Cannot find record " + idx)
            }
            readAllFrom(s) must equal (allBytes)
          }
        }
      }
    }
  }

  test("Can read out what is written, in the same order it was written") {
    roundtripTest(shuffleWrites = false, shuffleReads = false)
  }

  test("Can read out what is written, in a random order") {
    roundtripTest(shuffleWrites = false, shuffleReads = true)
  }

  test("Can write in a random order, then read in order") {
    roundtripTest(shuffleWrites = true, shuffleReads = false)
  }

  test("Can write in a random order, then read in another random order") {
    roundtripTest(shuffleWrites = true, shuffleReads = true)
  }
}
