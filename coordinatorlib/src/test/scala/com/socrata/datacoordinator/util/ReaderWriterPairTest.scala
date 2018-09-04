package com.socrata.datacoordinator.util

import java.util.concurrent.ArrayBlockingQueue

import org.scalatest.FunSuite
import org.scalatest.MustMatchers
import org.scalatest.prop.PropertyChecks
import org.scalacheck.{Arbitrary, Gen}
import java.io.IOException

class ReaderWriterPairTest extends FunSuite with MustMatchers with PropertyChecks {
  test("Should be able to read and write") {
    forAll(implicitly[Arbitrary[Array[String]]].arbitrary, Gen.choose(1, 1000), Gen.choose(1, 1000)) { (ss, size, count) =>
      whenever(size > 0 && count > 0) {
        val rwp = new ReaderWriterPair(size, count)
        val readResult = new ArrayBlockingQueue[String](1)
        val readerThread = new Thread {
          setName("Reader thread")
          override def run(): Unit = {
            val buf = new Array[Char](10)
            val sb = new StringBuilder
            def loop(): Unit = {
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

        readResult.take().map(_.toInt) must equal (ss.mkString.map(_.toInt))
      }
    }
  }

  test("Writing when the reader is closed throws") {
    forAll(implicitly[Arbitrary[Array[String]]].arbitrary, Gen.choose(1, 1000), Gen.choose(1, 1000)) { (ss, size, count) =>
      whenever(size > 0 && count > 0 && ss.exists(_.nonEmpty)) {
        val rwp = new ReaderWriterPair(size, count)
        rwp.reader.close()
        an [IOException] must be thrownBy {
          for(s <- ss) rwp.writer.write(s)
          rwp.writer.flush()
        }
      }
    }
  }
}
