package com.socrata.datacoordinator.common.soql.obfuscation

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import org.scalatest.prop.PropertyChecks
import com.socrata.datacoordinator.id.RowId

class RowIdObfuscatorTest extends FunSuite with MustMatchers with PropertyChecks {
  test("deobfuscate-of-obfuscate is the identity") {
    forAll { (x: Long, key: Array[Byte]) =>
      whenever(key.nonEmpty) {
        val cryptProvider = new CryptProvider(key)
        val rowId = new RowId(x)
        val obf = new RowIdObfuscator(cryptProvider)
        obf.deobfuscate(obf.obfuscate(rowId)) must equal (Some(rowId))
      }
    }
  }
}
