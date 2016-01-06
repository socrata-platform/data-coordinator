package com.socrata.querycoordinator.caching.cache.noop

import java.io.OutputStream

import com.google.common.io.CountingOutputStream
import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.querycoordinator.caching.cache.{ValueRef, CacheSession}
import org.apache.commons.io.output.NullOutputStream

final abstract class NoopCacheSession

object NoopCacheSession extends CacheSession {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[NoopCacheSession])

  override def find(key: String, resourceScope: ResourceScope): Option[ValueRef] = None

  override def create(key: String)(filler: OutputStream => Unit): Unit = {
    val out = new CountingOutputStream(NullOutputStream.NULL_OUTPUT_STREAM)
    filler(out)
    log.info("Would have cached {} byte(s) as {}, but am not because noop", out.getCount, key)
  }
}
