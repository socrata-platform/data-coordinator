package com.socrata.querycoordinator.caching

import org.scalatest.FunSuite
import org.scalatest.MustMatchers
import org.scalatest.prop.PropertyChecks

import java.io.{InputStream, ByteArrayInputStream, ByteArrayOutputStream}

import scala.annotation.tailrec

class CircularByteBufferTest extends FunSuite with MustMatchers with PropertyChecks {
  test("Putting bytes and then reading them without overflowing works") {
    forAll { (start: Int, extra: Int, bss0: List[List[Byte]]) =>
      val bss = bss0.map(_.toArray)
      val totalSize = bss.map(_.length).sum + (extra & 0xf) + 1
      whenever(totalSize > 1) {
        val buf = new CircularByteBuffer(totalSize)
        buf.clearAt(start)
        for(bs <- bss) buf.put(bs) must equal (bs.length)
        val baos = new ByteArrayOutputStream
        buf.writeTo(baos) must equal (bss.map(_.length).sum)
        baos.toByteArray must equal (bss.toArray.flatten)
      }
    }
  }

  class ShortReadingByteArrayInputStream(bss: List[Array[Byte]]) extends InputStream {
    var remaining = bss.drop(1)
    var current = bss.headOption.map(new ByteArrayInputStream(_))

    @tailrec
    final def read(): Int =
      current match {
        case None => -1
        case Some(bais) =>
          bais.read() match {
            case -1 =>
              advance(); read()
            case b =>
              b
          }
      }

    def advance(): Unit = {
      current = remaining.headOption.map(new ByteArrayInputStream(_))
      remaining = remaining.drop(1)
    }

    override def read(bs: Array[Byte], offset: Int, length: Int): Int = {
      current match {
        case None => -1
        case Some(bais) =>
          bais.read(bs, offset, length) match {
            case -1 =>
              advance(); read(bs, offset, length)
            case n =>
              n
          }
      }
    }
  }

  test("Reading bytes and then reading them without overflowing works") {
    forAll { (start: Int, extra: Int, bss0: List[List[Byte]]) =>
      val bss = bss0.filterNot(_.isEmpty).map(_.toArray)
      val totalSize = bss.map(_.length).sum + (extra & 0xf) + 1
      whenever(totalSize > 1) {
        val in = new ShortReadingByteArrayInputStream(bss)
        val buf = new CircularByteBuffer(totalSize)
        buf.clearAt(start)
        Iterator.continually(buf.read(in, totalSize)).takeWhile(_ > 0).sum must equal (bss.map(_.length).sum)
        val baos = new ByteArrayOutputStream
        buf.writeTo(baos) must equal (bss.map(_.length).sum)
        baos.toByteArray must equal (bss.toArray.flatten)
      }
    }
  }

  test("available - split data") {
    forAll { (bss0: List[Byte], bss1: List[Byte], space0: Int) =>
      val space = space0 & 0xfff
      val buf = CircularByteBuffer.splitData(bss0.length, space, (bss0 ++ bss1).toArray)
      buf.available must equal (bss0.length + bss1.length)
    }
  }

  test("available - split space") {
    forAll { (space0: Int, bss0: List[Byte], space1: Int) =>
      val preSpace = space0 & 0xfff
      val postSpace = space1 & 0xfff
      val buf = CircularByteBuffer.splitSpace(preSpace, bss0.toArray, postSpace)
      buf.available must equal (bss0.length)
    }
  }

  test("space - split data") {
    forAll { (bss0: List[Byte], bss1: List[Byte], space0: Int) =>
      val space = space0 & 0xfff
      val buf = CircularByteBuffer.splitData(bss0.length, space, (bss0 ++ bss1).toArray)
      buf.space must equal (space)
    }
  }

  test("space - split space") {
    forAll { (space0: Int, bss0: List[Byte], space1: Int) =>
      val preSpace = space0 & 0xfff
      val postSpace = space1 & 0xfff
      val buf = CircularByteBuffer.splitSpace(preSpace, bss0.toArray, postSpace)
      buf.space must equal (preSpace + postSpace)
    }
  }

  test("split data can get all data") {
    forAll { (bss0: List[Byte], bss1: List[Byte], space0: Int) =>
      val space = space0 & 0xfff
      val buf = CircularByteBuffer.splitData(bss0.length, space, (bss0 ++ bss1).toArray)
      val bs = new Array[Byte](bss0.length + bss1.length)
      buf.get(bs) must equal (bs.length)
      bs.toList must equal (bss0 ++ bss1)
      buf.available must equal (0)
    }
  }

  test("Overflow works -- unsplit") {
    val buf = new CircularByteBuffer(10)
    buf.put(Array[Byte](1,2,3,4,5,6,7,8,9,10)) must equal (10)
    buf.put(Array[Byte](11)) must equal (0)
    buf.getAll must equal (Array[Byte](1,2,3,4,5,6,7,8,9,10))
  }

  test("Overflow works -- unsplit 2") {
    val buf = new CircularByteBuffer(10)
    buf.clearAt(1)
    buf.put(Array[Byte](1,2,3,4,5,6,7,8,9,10)) must equal (10)
    buf.put(Array[Byte](11)) must equal (0)
    buf.getAll must equal (Array[Byte](1,2,3,4,5,6,7,8,9,10))
  }

  test("Overflow works -- split") {
    val buf = new CircularByteBuffer(10)
    buf.clearAt(5)
    buf.put(Array[Byte](1,2,3,4,5,6,7,8,9,10)) must equal (10)
    buf.put(Array[Byte](11)) must equal (0)
    buf.getAll must equal (Array[Byte](1,2,3,4,5,6,7,8,9,10))
  }
}
