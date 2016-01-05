package com.socrata.querycoordinator.caching.cache.postgresql

import javax.sql.DataSource
import com.rojoma.simplearm.v2._

object PostgresqlCacheSchema {
  def create(dataSource: DataSource): Unit = {
    using(dataSource.getConnection()) { conn =>
      conn.setAutoCommit(false)

      try {
        val md = conn.getMetaData

        def exists(table: String): Boolean =
          using(md.getTables(conn.getCatalog, "public", table, Array("TABLE"))) { rs =>
            rs.next()
          }

        using(conn.createStatement()) { stmt =>
          // ok, so "cache" is sort of interesting.  There's a unique constraint on "key" but it is nullable; there can
          // be more than one null value in a column with a unique constraint, and we're taking advantage of that.  A
          // row with a null key is either being created or pending deletion; the decision is based on a combination of
          // being referred to from the pending_deletes table (in which case it's _definitely_ pending delete) or having
          // an old "created_at" timestamp (in which case likely something happened to prevent the population of the data
          // from being completed).
          //
          // pending_deletes exists so that we can mark an entry for deletion and be (reasonably) assured that anything
          // currently using that entry will finish successfully.  The actual delete happens asynchronously.
          if(!exists("cache")) {
            stmt.execute("create table cache (id bigserial not null primary key, key text unique, created_at timestamp with time zone not null, approx_last_access timestamp with time zone not null)")
            stmt.execute("create index cache_created_at_idx on cache (created_at)")
            stmt.execute("create index cache_access_idx on cache (approx_last_access)")
          }
          if(!exists("cache_data")) {
            stmt.execute("create table cache_data (cache_id bigint not null references cache (id), chunknum int not null, data bytea not null, primary key(cache_id, chunknum))")
          }
          if(!exists("pending_deletes")) {
            stmt.execute("create table pending_deletes (id bigserial not null primary key, cache_id bigint not null references cache(id), delete_queued timestamp with time zone not null)")
            stmt.execute("create index pending_deletes_delete_queued_idx on pending_deletes(delete_queued)")
          }
        }

        conn.commit()
      } finally {
        conn.rollback()
      }
    }
  }
}
