package com.socrata.querycoordinator.caching

import scala.language.implicitConversions
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

object Hasher {
  trait ImplicitlyByteable {
    def asBytes: Array[Byte]
  }

  object ImplicitlyByteable {
    implicit def implicitlyByteable(s: String): ImplicitlyByteable = new ImplicitlyByteable {
      override def asBytes: Array[Byte] = s.getBytes(StandardCharsets.UTF_8)
    }

    implicit def implicitlyByteable(bs: Array[Byte]): ImplicitlyByteable = new ImplicitlyByteable {
      override def asBytes: Array[Byte] = bs.clone()
    }

    implicit def implicitlyBytable(optStr: Option[String]): ImplicitlyByteable = new ImplicitlyByteable {
      override def asBytes: Array[Byte] = optStr.toString.getBytes(StandardCharsets.UTF_8)
    }

    implicit def implicitlyBytable(n: Long): ImplicitlyByteable = new ImplicitlyByteable {
      override def asBytes: Array[Byte] = {
        val os = new Array[Byte](8)
        os(0) = (n >> 56).toByte
        os(1) = (n >> 48).toByte
        os(2) = (n >> 40).toByte
        os(3) = (n >> 32).toByte
        os(4) = (n >> 24).toByte
        os(5) = (n >> 16).toByte
        os(6) = (n >> 8).toByte
        os(7) = n.toByte
        os
      }
    }
  }

  def hash(items: ImplicitlyByteable*): Array[Byte] = {
    val md = MessageDigest.getInstance("SHA-256")
    val lenBuf = new Array[Byte](4)
    items.foreach { item =>
      val bs = item.asBytes
      val len = bs.length
      lenBuf(0) = (len >> 24).toByte
      lenBuf(1) = (len >> 16).toByte
      lenBuf(2) = (len >> 8).toByte
      lenBuf(3) = len.toByte
      md.update(lenBuf)
      md.update(bs)
    }
    md.digest()
  }
}
