package com.socrata.datacoordinator.util

import com.rojoma.json.codec.JsonCodec
import scala.concurrent.duration.FiniteDuration

trait Cache {
  def cache[T : JsonCodec](key: Seq[String], value: T, lifetimeHint: FiniteDuration)
  def lookup[T : JsonCodec](key: Seq[String]): Option[T]
}

final abstract class NullCache

object NullCache extends Cache {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[NullCache])
  def cache[T : JsonCodec](key: Seq[String], value: T, lifetimeHint: FiniteDuration) {
    log.info("Failing to cache {} at {}", JsonCodec[T].encode(value) : Any, key.mkString("/") : Any)
  }
  def lookup[T : JsonCodec](key: Seq[String]): Option[T] = {
    log.info("Failing to retrieve {}", key.mkString("/"))
    None
  }
}
