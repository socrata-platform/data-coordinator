package com.socrata.datacoordinator.util

import java.util.concurrent.ArrayBlockingQueue

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import org.scalatest.prop.PropertyChecks
import org.scalacheck.{Arbitrary, Gen}

class ReaderWriterPairTest extends FunSuite with MustMatchers with PropertyChecks {
  test("Should be able to read and write") {
    forAll(implicitly[Arbitrary[Array[String]]].arbitrary, Gen.choose(2, 1000)) { (ss, size) =>
      whenever(size > 1) {
        val rwp = new ReaderWriterPair(size)
        val readResult = new ArrayBlockingQueue[String](1)
        val readerThread = new Thread {
          setName("Reader thread")
          override def run() {
            val buf = new Array[Char](10)
            val sb = new StringBuilder
            def loop() {
              val count = rwp.reader.read(buf)
              if(count != -1) {
                sb.appendAll(buf, 0, count)
                loop()
              }
            }
            loop()
            readResult.add(sb.toString)
          }
        }
        readerThread.start()

        for(s <- ss) rwp.writer.write(s)
        rwp.writer.close()

        readResult.take() must equal (ss.mkString)
      }
    }
  }

  test("Writing when the reader is closed does not block") {
    forAll(implicitly[Arbitrary[Array[String]]].arbitrary, Gen.choose(2, 1000)) { (ss, size) =>
      whenever(size > 1) {
        val rwp = new ReaderWriterPair(size)
        rwp.reader.close()
        for(s <- ss) rwp.writer.write(s)
      }
    }
  }
}
