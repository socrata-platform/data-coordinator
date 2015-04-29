package com.socrata.datacoordinator.truth.loader

import org.scalatest.{Assertions, FunSuite}
import org.scalatest.MustMatchers

class DeloggerTest extends FunSuite with MustMatchers with Assertions {
  import Delogger._

  test("Log events and their companions must be consistent") {
    import scala.reflect.runtime.universe._

    val classes = typeOf[LogEvent[_]].typeSymbol.asClass.knownDirectSubclasses
    classes must not be 'empty

    val companions = classes.map { sym =>
      sym.asClass.companionSymbol.name.toString
    }
    classes.size must equal (companions.size)

    assert(allLogEventCompanions.size == companions.size,
      "An entry is missing from the allLogEventCompanions set")

    companionFromProductName.size must equal (classes.size)
    assert(companionFromProductName.keySet == companions,
      s"Different:\n${companionFromProductName.keySet.toSeq.sorted}\n${companions.toSeq.sorted}}")
  }

  test("productNameFromCompanion and companionFromProductName are inverses") {
    productNameFromCompanion.keySet must equal (companionFromProductName.values.toSet)
    companionFromProductName.keySet must equal (productNameFromCompanion.values.toSet)
  }
}
