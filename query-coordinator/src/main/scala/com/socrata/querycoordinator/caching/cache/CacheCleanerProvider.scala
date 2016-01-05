package com.socrata.querycoordinator.caching.cache

trait CacheCleanerProvider {
  def cleaner(): CacheCleaner
}
