package com.socrata.datacoordinator.service

import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import java.security.MessageDigest
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.environment.TypeName
import java.util.Comparator

object SchemaHash {
  private val hexDigit = "0123456789abcdef".toCharArray

  private def hexString(xs: Array[Byte]) = {
    val cs = new Array[Char](xs.length * 2)
    var i = 0
    while(i != xs.length) {
      val dst = i << 1
      cs(dst) = hexDigit((xs(i) >> 4) & 0xf)
      cs(dst+1) = hexDigit(xs(i) & 0xf)
      i += 1
    }
    new String(cs)
  }

  def computeHash[CT](schema: ColumnIdMap[ColumnInfo[CT]], typeSerializer: CT => TypeName): String = {
    val sha1 = MessageDigest.getInstance("SHA-1")
    val cols = schema.values.toArray
    java.util.Arrays.sort(cols, new Comparator[ColumnInfo[CT]] {
      def compare(a: ColumnInfo[CT], b: ColumnInfo[CT]) =
        a.logicalName compare b.logicalName
    })
    for(col <- cols) {
      sha1.update(col.logicalName.caseFolded.getBytes("UTF-8"))
      sha1.update(255.toByte)
      sha1.update(typeSerializer(col.typ).caseFolded.getBytes("UTF-8"))
      sha1.update((if(col.isSystemPrimaryKey) 255 else 254).toByte)
      sha1.update((if(col.isUserPrimaryKey) 255 else 254).toByte)
    }
    hexString(sha1.digest())
  }
}
