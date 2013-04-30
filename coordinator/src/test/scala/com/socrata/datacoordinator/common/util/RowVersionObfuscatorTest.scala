package com.socrata.datacoordinator.common.util

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import org.scalatest.prop.PropertyChecks
import com.socrata.datacoordinator.id.RowVersion

class RowVersionObfuscatorTest extends FunSuite with MustMatchers with PropertyChecks {
  test("deobfuscate-of-obfuscate is the identity") {
    forAll { (x: Long, key: Array[Byte]) =>
      whenever(key.nonEmpty) {
        val cryptProvider = new CryptProvider(key)
        val rowVersion = new RowVersion(x)
        val obf = new RowVersionObfuscator(cryptProvider)
        obf.deobfuscate(obf.obfuscate(rowVersion)) must equal (Some(rowVersion))
      }
    }
  }
}
