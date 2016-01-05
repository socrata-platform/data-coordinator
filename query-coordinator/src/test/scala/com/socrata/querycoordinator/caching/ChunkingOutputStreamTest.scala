package com.socrata.querycoordinator.caching

import org.scalatest.prop.PropertyChecks
import org.scalatest.{FunSuite, MustMatchers}

import scala.collection.mutable.ArrayBuffer

class ChunkingOutputStreamTest extends FunSuite with MustMatchers with PropertyChecks {
  test("ChunkingOutputStream chunkifies") {
    forAll { (chunkSize0: Int, bss: List[List[Byte]]) =>
      val chunkSize = (chunkSize0 & 0xff) + 1
      val chunks = new ArrayBuffer[Array[Byte]]
      val cos = new ChunkingOutputStream(chunkSize) {
        override def onChunk(bytes: Array[Byte]): Unit = chunks += bytes
      }
      for(bs <- bss) cos.write(bs.toArray)
      cos.close()
      val result = chunks.toArray
      result.dropRight(1).foreach { bs => bs.length must equal (chunkSize) }
      result.takeRight(1).foreach { bs => bs.length must be <= chunkSize }
      result.flatten must equal (bss.map(_.toArray).toArray.flatten)
    }
  }
}
