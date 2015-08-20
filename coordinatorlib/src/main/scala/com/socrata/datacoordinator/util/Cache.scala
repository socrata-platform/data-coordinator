package com.socrata.datacoordinator.util

import com.rojoma.json.v3.codec.JsonEncode
import scala.concurrent.duration.FiniteDuration

trait Cache {
  def cache[T : JsonEncode](key: Seq[String], value: T, lifetimeHint: FiniteDuration)
  def lookup[T](key: Seq[String]): Option[T]
}

final abstract class NullCache

object NullCache extends Cache {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[NullCache])
  def cache[T : JsonEncode](key: Seq[String], value: T, lifetimeHint: FiniteDuration) {
    log.debug("Failing to cache {} at {}", JsonEncode[T].encode(value) : Any, key.mkString("/") : Any)
  }
  def lookup[T](key: Seq[String]): Option[T] = {
    log.debug("Failing to retrieve {}", key.mkString("/"))
    None
  }
}
