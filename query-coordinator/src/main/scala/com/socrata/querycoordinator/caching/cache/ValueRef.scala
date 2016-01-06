package com.socrata.querycoordinator.caching.cache

import java.io.{InputStreamReader, Reader, InputStream}
import java.nio.charset.StandardCharsets

import com.rojoma.simplearm.v2.ResourceScope

trait ValueRef extends AutoCloseable {
  /** The returned `InputStream` is valid until either the given `ResourceScope` closes
    * it or this `ValueRef` is closed, whichever comes first.
    */
  def open(resourceScope: ResourceScope): InputStream

  final def openText(resourceScope: ResourceScope): Reader = {
    val is = open(resourceScope)
    resourceScope.open(new InputStreamReader(is, StandardCharsets.UTF_8), transitiveClose = List(is))
  }
}
