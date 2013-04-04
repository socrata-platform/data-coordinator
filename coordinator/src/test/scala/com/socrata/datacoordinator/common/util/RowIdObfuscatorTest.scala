package com.socrata.datacoordinator.common.util

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import org.scalatest.prop.PropertyChecks
import com.socrata.datacoordinator.id.RowId

class RowIdObfuscatorTest extends FunSuite with MustMatchers with PropertyChecks {
  test("deobfuscate-of-obfuscate is the identity") {
    forAll { (x: Long, key: Array[Byte]) =>
      whenever(key.nonEmpty) {
        val rowId = new RowId(x)
        val obf = new RowIdObfuscator(key)
        obf.deobfuscate(obf.obfuscate(rowId)) must equal (Some(rowId))
      }
    }
  }
}
