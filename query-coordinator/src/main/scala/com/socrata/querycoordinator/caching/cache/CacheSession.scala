package com.socrata.querycoordinator.caching.cache

import java.io.{BufferedWriter, OutputStreamWriter, Writer, OutputStream}
import java.nio.charset.StandardCharsets

import com.rojoma.simplearm.v2._

/**
 * A session is NOT necessarily thread-safe.  It can be used from multiple threads
 * (there is no hidden thread-local state) but not concurrently.
 */
trait CacheSession {
  /** Looks up a key by name.  The returned ValueRef is valid until given
    * `ResourceScope` closes it or this cache session is closed, whichever
    * comes first.
    */
  def find(key: String, resourceScope: ResourceScope): Option[ValueRef]

  /** Create or replace a key.  It will not be visible until `filler` returns,
    * and MAY not be visible for some time thereafter. */
  def create(key: String)(filler: OutputStream => Unit)

  final def createText(key: String)(filler: Writer => Unit) =
    create(key) { os =>
      using(new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8)))(filler)
    }
}
