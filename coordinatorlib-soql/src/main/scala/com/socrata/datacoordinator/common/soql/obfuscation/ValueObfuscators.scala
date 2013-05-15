package com.socrata.datacoordinator.common.soql.obfuscation

import java.security.SecureRandom

import org.bouncycastle.crypto.params.KeyParameter
import org.bouncycastle.crypto.engines.BlowfishEngine
import org.bouncycastle.crypto.BlockCipher

import com.socrata.datacoordinator.common.soql.SoQLRep
import com.socrata.datacoordinator.id.{RowVersion, RowId}

class CryptProvider(key: Array[Byte]) {
  private val keyParam = new KeyParameter(key)

  lazy val encryptor = locally {
    val bf = new BlowfishEngine
    bf.init(true, keyParam)
    bf
  }

  lazy val decryptor = locally {
    val bf = new BlowfishEngine
    bf.init(false, keyParam)
    bf
  }
}

object CryptProvider {
  private val rng = new SecureRandom

  def generateKey(): Array[Byte] = {
    val bs = new Array[Byte](72)
    rng.nextBytes(bs)
    bs
  }
}

abstract class Obfuscator(cryptProvider: CryptProvider) {
  private val formatter = LongFormatter
  private val in = new Array[Byte](8)
  private val out = new Array[Byte](8)
  private var _encryptor: BlowfishEngine = null
  private var _decryptor: BlowfishEngine = null
  protected val prefix: String

  def encryptor = {
    if(_encryptor == null) _encryptor = cryptProvider.encryptor
    _encryptor
  }

  private def decryptor = {
    if(_decryptor == null) _decryptor = cryptProvider.decryptor
    _decryptor
  }

  private def byteify(bs: Array[Byte], x: Long) {
    bs(0) = x.toByte
    bs(1) = (x >> 8).toByte
    bs(2) = (x >> 16).toByte
    bs(3) = (x >> 24).toByte
    bs(4) = (x >> 32).toByte
    bs(5) = (x >> 40).toByte
    bs(6) = (x >> 48).toByte
    bs(7) = (x >> 56).toByte
  }

  private def debyteify(bs: Array[Byte]): Long =
    (bs(0) & 0xff).toLong +
      ((bs(1) & 0xff).toLong << 8) +
      ((bs(2) & 0xff).toLong << 16) +
      ((bs(3) & 0xff).toLong << 24) +
      ((bs(4) & 0xff).toLong << 32) +
      ((bs(5) & 0xff).toLong << 40) +
      ((bs(6) & 0xff).toLong << 48) +
      ((bs(7) & 0xff).toLong << 56)

  // This SHOULDN'T ever be called from multiple threads.  In that case, HotSpot should
  // be able to keep the overhead of the synchronized block down to a minimum.
  private def crypt(x: Long, engine: BlockCipher): Long = synchronized {
    byteify(in, x)
    engine.processBlock(in, 0, out, 0)
    debyteify(out)
  }

  protected def encrypt(value: Long) =
    prefix + formatter.format(crypt(value, encryptor))

  protected def decrypt(value: String): Option[Long] =
    if(value.startsWith(prefix)) {
      formatter.deformat(value, prefix.length).map { x =>
        crypt(x, decryptor)
      }
    } else {
      None
    }
}

final class RowIdObfuscator(cryptProvider: CryptProvider) extends Obfuscator(cryptProvider) with SoQLRep.IdObfuscationContext {
  protected val prefix = "row-"

  def obfuscate(rowId: RowId): String =
    encrypt(rowId.underlying)

  def deobfuscate(obfuscatedRowId: String): Option[RowId] =
    decrypt(obfuscatedRowId).map(new RowId(_))
}

final class RowVersionObfuscator(cryptProvider: CryptProvider) extends Obfuscator(cryptProvider) with SoQLRep.VersionObfuscationContext {
  protected val prefix = "rv-"

  def obfuscate(rowVersion: RowVersion): String =
    encrypt(rowVersion.underlying)

  def deobfuscate(obfuscatedRowVersion: String): Option[RowVersion] =
    decrypt(obfuscatedRowVersion).map(new RowVersion(_))
}
