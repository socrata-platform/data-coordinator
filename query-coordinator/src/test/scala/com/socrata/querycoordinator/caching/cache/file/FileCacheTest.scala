package com.socrata.querycoordinator.caching.cache.file

import scala.concurrent.duration._
import java.io.BufferedReader

import com.socrata.querycoordinator.util.TemporaryDirectory
import org.scalatest.{MustMatchers, FunSuite}
import com.rojoma.simplearm.v2._

class FileCacheTest extends FunSuite with MustMatchers {
  test("Basic operation works") {
    using(new ResourceScope) { rs =>
      val dir = TemporaryDirectory.scoped(rs)

      val sp = new FileCacheSessionProvider(dir, 5.minutes, 5.minutes, 5.minutes)
      sp.init()

      val session = sp.open(rs)
      session.find("dne", rs) must equal (None)
      session.createText("hello") { w =>
        w.write("world\n")
      }

      session.find("hello", rs) match {
        case Some(in) =>
          val r = new BufferedReader(in.openText(rs))
          r.readLine() must equal ("world")
          r.readLine() must be (null)

          // this will only actually open the file once, but will produce a second stream atop the same
          // underlying FileChannel.
          val r2 = new BufferedReader(in.openText(rs))
          r2.readLine() must equal ("world")
          r2.readLine() must be (null)
        case None =>
          fail("should have found `hello'")
      }
    }
  }
}
