package com.socrata.datacoordinator.common.soql.obfuscation

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import org.scalatest.prop.PropertyChecks

class LongFormatterTest extends FunSuite with MustMatchers with PropertyChecks {
  test("Format always produces exactly 14 characters") {
    forAll { x: Long =>
      LongFormatter.format(x).length must be (14)
    }
  }

  test("Deformat-format is the identity") {
    forAll { x: Long =>
      LongFormatter.deformatEx(LongFormatter.format(x)) must equal (x)
    }
  }
}
