package com.socrata.querycoordinator.caching.cache.config

import com.rojoma.simplearm.v2._
import com.socrata.querycoordinator.caching.StreamWrapper
import com.socrata.querycoordinator.caching.cache.noop.NoopCacheSessionProvider
import com.socrata.querycoordinator.caching.cache.{CacheInit, CacheCleanerProvider, CacheSessionProvider}
import com.socrata.querycoordinator.caching.cache.file.FileCacheSessionProvider
import com.socrata.querycoordinator.caching.cache.postgresql.PostgresqlCacheSessionProvider
import org.postgresql.ds.PGPoolingDataSource

object CacheSessionProviderFromConfig {
  def apply(config: CacheConfig, streamWrapper: StreamWrapper = StreamWrapper.noop): Managed[CacheSessionProvider with CacheCleanerProvider with CacheInit] =
    config.realConfig match {
      case fs: FilesystemCacheConfig =>
        fromFilesystem(fs, streamWrapper)
      case pg: PostgresqlCacheConfig =>
        fromPostgres(pg, streamWrapper)
      case NoopCacheConfig =>
        unmanaged(NoopCacheSessionProvider)
    }

  def fromFilesystem(fs: FilesystemCacheConfig, streamWrapper: StreamWrapper) =
    unmanaged(new FileCacheSessionProvider(
      fs.root,
      updateATimeInterval = fs.atimeUpdateInterval,
      survivorCutoff = fs.survivorCutoff,
      assumeDeadCreateCutoff = fs.assumeDeadCreateCutoff,
      streamWrapper = streamWrapper))

  def fromPostgres(pg: PostgresqlCacheConfig, streamWrapper: StreamWrapper) =
    for(ds <- managed(new PGPoolingDataSource)) yield {
      ds.setDatabaseName(pg.db.database)
      ds.setUser(pg.db.username)
      ds.setPassword(pg.db.password)
      ds.setServerName(pg.db.host)
      ds.setInitialConnections(10)
      ds.setMaxConnections(30)
      new PostgresqlCacheSessionProvider(ds,
        updateATimeInterval = pg.atimeUpdateInterval,
        survivorCutoff = pg.survivorCutoff,
        assumeDeadCreateCutoff = pg.assumeDeadCreateCutoff,
        deleteDelay = pg.deleteDelay,
        streamWrapper = streamWrapper)
    }
}
