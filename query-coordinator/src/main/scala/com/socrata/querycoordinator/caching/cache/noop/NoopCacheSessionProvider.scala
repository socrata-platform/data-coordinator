package com.socrata.querycoordinator.caching.cache.noop

import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.querycoordinator.caching.cache._

object NoopCacheSessionProvider extends CacheSessionProvider with CacheCleanerProvider with CacheInit {
  override def open(rs: ResourceScope): CacheSession = NoopCacheSession

  override def cleaner(): CacheCleaner = NoopCacheCleaner

  override def init(): Unit = {}
}
