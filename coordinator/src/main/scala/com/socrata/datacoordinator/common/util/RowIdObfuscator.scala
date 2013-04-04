package com.socrata.datacoordinator.common.util

import com.socrata.datacoordinator.common.soql.SoQLRep
import com.socrata.datacoordinator.id.RowId
import org.bouncycastle.crypto.engines.BlowfishEngine
import org.bouncycastle.crypto.params.KeyParameter
import org.bouncycastle.crypto.BlockCipher

class RowIdObfuscator(key: Array[Byte]) extends SoQLRep.IdObfuscationContext {
  private val formatter = LongFormatter
  private val in = new Array[Byte](8)
  private val out = new Array[Byte](8)
  private var _encryptor: BlowfishEngine = null
  private var _decryptor: BlowfishEngine = null
  private val keyParam = new KeyParameter(key)
  private val prefix = "row-"

  private def initEncryptor() {
    _encryptor = new BlowfishEngine
    _encryptor.init(true, keyParam)
  }

  private def initDecryptor() {
    _decryptor = new BlowfishEngine
    _decryptor.init(false, keyParam)
  }

  private def encryptor = {
    if(_encryptor == null) initEncryptor()
    _encryptor
  }

  private def decryptor = {
    if(_decryptor == null) initDecryptor()
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

  private def crypt(x: Long, engine: BlockCipher): Long = synchronized {
    byteify(in, x)
    engine.processBlock(in, 0, out, 0)
    debyteify(out)
  }

  def obfuscate(rowId: RowId): String =
    prefix + formatter.format(crypt(rowId.underlying, encryptor))

  def deobfuscate(obfuscatedRowId: String): Option[RowId] =
    if(obfuscatedRowId.startsWith(prefix)) {
      formatter.deformat(obfuscatedRowId.substring(prefix.length)).map { x =>
        new RowId(crypt(x, decryptor))
      }
    } else {
      None
    }
}
