package com.socrata.querycoordinator.caching.cache

import com.rojoma.simplearm.v2.ResourceScope

trait CacheSessionProvider {
  def open(rs: ResourceScope): CacheSession
}
