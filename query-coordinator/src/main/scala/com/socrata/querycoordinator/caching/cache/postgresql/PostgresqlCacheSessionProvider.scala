package com.socrata.querycoordinator.caching.cache.postgresql

import javax.sql.DataSource

import com.rojoma.simplearm.v2._
import com.socrata.querycoordinator.caching.cache._
import com.socrata.util.io.StreamWrapper

import scala.concurrent.duration.FiniteDuration

class PostgresqlCacheSessionProvider(ds: DataSource,
                                     updateATimeInterval: FiniteDuration,
                                     survivorCutoff: FiniteDuration,
                                     deleteDelay: FiniteDuration,
                                     assumeDeadCreateCutoff: FiniteDuration,
                                     streamWrapper: StreamWrapper = StreamWrapper.noop)
  extends CacheSessionProvider with CacheCleanerProvider with CacheInit
{
  private object JdbcCacheSessionResource extends Resource[PostgresqlCacheSession] {
    override def close(a: PostgresqlCacheSession): Unit = a.close()
    override def closeAbnormally(a: PostgresqlCacheSession, e: Throwable) = a.closeAbnormally()
  }

  override def init(): Unit = PostgresqlCacheSchema.create(ds)

  override def open(rs: ResourceScope): CacheSession =
    rs.open(new PostgresqlCacheSession(ds, updateATimeInterval, streamWrapper))(JdbcCacheSessionResource)

  def cleaner(): CacheCleaner =
    new PostgresqlCacheCleaner(ds, survivorCutoff, deleteDelay, assumeDeadCreateCutoff)
}
