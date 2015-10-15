package com.socrata.datacoordinator.common

import java.security.SecureRandom

object StandardObfuscationKeyGenerator extends (() => Array[Byte]) {
  val rng = new SecureRandom
  val len = 72 /* Magic */

  def apply(): Array[Byte] = {
    val cs = new Array[Byte](len)
    rng.nextBytes(cs)
    cs
  }
}
