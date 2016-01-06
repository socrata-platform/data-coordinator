package com.socrata.querycoordinator.caching.cache.noop

import com.socrata.querycoordinator.caching.cache.CacheCleaner

final abstract class NoopCacheCleaner

object NoopCacheCleaner extends CacheCleaner {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[NoopCacheCleaner])

  override def clean(): Unit = {
    log.info("Would clean the cache, but am not because noop")
  }
}
