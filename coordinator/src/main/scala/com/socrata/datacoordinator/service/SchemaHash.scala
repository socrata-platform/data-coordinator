package com.socrata.datacoordinator.service

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.environment.TypeName
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
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

  def computeHash[CT](schema: ColumnIdMap[ColumnInfo[CT]], locale: String, typeSerializer: CT => TypeName): String = {
    val cols = schema.values.toArray
    val sha1 = MessageDigest.getInstance("SHA-1")

    sha1.update(locale.getBytes(UTF_8))
    sha1.update(255.toByte)

    val pk = cols.find(_.isUserPrimaryKey).orElse(cols.find(_.isSystemPrimaryKey)).getOrElse {
      sys.error("No primary key defined on dataset?")
    }
    sha1.update(pk.userColumnId.underlying.getBytes(UTF_8))
    sha1.update(255.toByte)

    java.util.Arrays.sort(cols, new Comparator[ColumnInfo[CT]] {
      val o = Ordering[UserColumnId]
      def compare(a: ColumnInfo[CT], b: ColumnInfo[CT]) =
        o.compare(a.userColumnId, b.userColumnId)
    })
    for(col <- cols) {
      sha1.update(col.userColumnId.underlying.getBytes(UTF_8))
      sha1.update(255.toByte)
      sha1.update(typeSerializer(col.typ).caseFolded.getBytes(UTF_8))
      sha1.update(255.toByte)
    }

    hexString(sha1.digest())
  }
}
