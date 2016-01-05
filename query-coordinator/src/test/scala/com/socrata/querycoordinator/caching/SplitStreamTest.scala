package com.socrata.querycoordinator.caching

import java.io.{InputStream, DataInputStream, ByteArrayInputStream}
import java.security.MessageDigest
import java.util.Random
import java.util.concurrent.CyclicBarrier

import com.rojoma.simplearm.v2._
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FunSuite, MustMatchers}

class SplitStreamTest extends FunSuite with MustMatchers with PropertyChecks {
  test("speedy can read the data") {
    forAll { (bs0: List[List[Byte]]) =>
      val bais = new ByteArrayInputStream(bs0.map(_.toArray).toArray.flatten)
      using(new ResourceScope) { rs =>
        val (speedy0, _) = SplitStream(bais, 10, rs)
        val speedy = new DataInputStream(speedy0)
        def loop(bs: List[List[Byte]]): Unit = {
          val bytes = new Array[Byte](bs.headOption.fold(0)(_.length))
          speedy.readFully(bytes)
          (bytes, bs) match {
            case (a, b :: tl) => a.toList must equal (b); loop(tl)
            case (Array(), Nil) => // ok
            case (a, Nil) => fail("Expected eof, got " + a.toList)
          }
        }
        loop(bs0)
        speedy.read() must equal(-1)
      }
    }
  }

  test("speedy can read the data and then the laggard can read the data") {
    forAll { (bs0: List[Byte]) =>
      val bais = new ByteArrayInputStream(bs0.toArray)
      using(new ResourceScope) { rs =>
        val (speedy0, laggard0) = SplitStream(bais, 10, rs)
        val speedy = new DataInputStream(speedy0)
        val laggard = new DataInputStream(laggard0)
        val bytes = new Array[Byte](bs0.length)
        speedy.readFully(bytes)
        bytes.toList must equal (bs0)
        speedy.read() must equal (-1)
        java.util.Arrays.fill(bytes, 0.toByte)
        laggard.readFully(bytes)
        laggard.read() must equal (-1)
        bytes.toList must equal(bs0)
      }
    }
  }

  test("speedy and laggard interleaved do not create any temp files if the buffer size is not exceeded") {
    forAll { (bss0: List[List[Byte]]) =>
      val bss = bss0.map(_.toArray)
      val bais = new ByteArrayInputStream(bss.toArray.flatten)
      whenever(bais.available != 0) {
        using(new ResourceScope) { rs =>
          val (speedy0, laggard0) = SplitStream(bais, bss.maxBy(_.length).length, rs)
          val speedy = new DataInputStream(speedy0)
          val laggard = new DataInputStream(laggard0)
          for(bs <- bss) {
            val bytes = new Array[Byte](bs.length)
            speedy.readFully(bytes)
            bytes must equal (bs)
            java.util.Arrays.fill(bytes, 0.toByte)
            laggard.readFully(bytes)
            bytes must equal (bs)
          }
          speedy.read() must equal (-1)
          laggard.read() must equal (-1)

          speedy0.filesCreated must equal(0)
        }
      }
    }
  }

  test("Only create one file if the laggard is slow enough") {
    val bais = new ByteArrayInputStream((1 to 1000).toArray.map(_.toByte))
    using(new ResourceScope) { rs =>
      val (speedy, laggard) = SplitStream(bais, 10, rs)
      while(speedy.read() != -1) {}
      laggard.filesCreated must equal (1)
    }
  }

  class RandomInputStream(n: Int, seed: Long) extends InputStream {
    private var remaining = n
    private val rng = new Random(seed)

    // There is a subtlety here!  We want to return the same bytestream
    // for a given seed, but the values returned from rng depend on
    // how they're chunked -- e.g., rng.nextBytes(2) ++ rng.nextBytes(2)
    // returns a different result from rng.nextBytes(4), because the
    // latter generates exactly one Int internally, whereas the former
    // generates two (and throws half of each away).  That's why we're not
    // using getBytes in the block read method.

    override def read(): Int = synchronized {
      if(remaining > 0) { remaining -= 1; rng.nextInt() & 0xff }
      else -1
    }

    override def read(bs: Array[Byte], off: Int, len: Int): Int = synchronized {
      if(remaining > 0) {
        val toGenerate = len min remaining
        remaining -= toGenerate
        def loop(n: Int): Unit = {
          if(n != toGenerate) {
            bs(off + n) = rng.nextInt().toByte
            loop(n+1)
          }
        }
        loop(0)
        toGenerate
      } else {
        -1
      }
    }
  }

  class HashingInputStream(algorithm: String, underlying: InputStream) extends InputStream {
    private val hash = MessageDigest.getInstance(algorithm)

    def digest(): Array[Byte] = synchronized { hash.digest() }

    override def read(): Int = {
      val bs = new Array[Byte](1)
      read(bs) match {
        case -1 => -1
        case 1 => bs(0) & 0xff
        case other => sys.error("read did not return a valid value")
      }
    }

    override def read(bs: Array[Byte], offset: Int, len: Int): Int = synchronized {
      underlying.read(bs, offset, len) match {
        case -1 => -1
        case n => hash.update(bs, offset, n); n
      }
    }

    override def close(): Unit = {
      underlying.close()
    }
  }

  test("Split streams are mutrally threadsafe") {
    val hash = "SHA-1"
    val count = 1000000
    val seed = 12345678L

    val data = new RandomInputStream(count, seed)
    using(new ResourceScope) { rs =>
      val (a0, b0) = SplitStream(data, 1500, rs)
      val a = new HashingInputStream(hash, a0)
      val b = new HashingInputStream(hash, b0)
      val barrier = new CyclicBarrier(2)
      val racer = new Thread {
        setName("Split stream test racer")
        setDaemon(true)
        override def run(): Unit = {
          val rng = new Random(54321)
          val buf = new Array[Byte](2000)
          barrier.await()
          while(b.read(buf, 0, rng.nextInt(buf.length)) != -1) { Thread.`yield`() }
          barrier.await()
        }
      }
      racer.start()

      locally {
        val rng = new Random(12345)
        val buf = new Array[Byte](2000)
        barrier.await()
        while(a.read(buf, 0, rng.nextInt(buf.length)) != -1) { Thread.`yield`() }
        barrier.await()
      }

      val noRaceData = new HashingInputStream(hash, new RandomInputStream(count, seed))
      locally {
        val buf = new Array[Byte](10000)
        while(noRaceData.read(buf) != -1) {}
      }

      val canonical = noRaceData.digest()
      val aResult = a.digest()
      val bResult = b.digest()
      aResult must equal (canonical)
      bResult must equal (canonical)
    }
  }
}
