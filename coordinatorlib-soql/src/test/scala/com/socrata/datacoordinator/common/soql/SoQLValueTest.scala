package com.socrata.datacoordinator.common.soql

import org.scalatest.{Assertions, FunSuite}
import org.scalatest.matchers.MustMatchers

class SoQLValueTest extends FunSuite with MustMatchers with Assertions {
  test("Companions must be consistent") {
    import scala.reflect.runtime.universe._
    val mirror = runtimeMirror(this.getClass.getClassLoader)

    val soqlType = typeOf[SoQLValue]
    soqlType.toString // Otherwise it's not initialized???

    val classes = soqlType.typeSymbol.asClass.knownDirectSubclasses.map(_.asClass)
    classes must not be ('empty)

    classes.foreach { sym =>
      val companionType = sym.typeSignature.member(newTermName("companion")).asMethod.returnType
      val companionModule = sym.asClass.companionSymbol.asModule
      assert(companionType <:< companionModule.typeSignature, "companion method did not point at the companion")
    }

    val companions = classes.map { sym =>
      val companionModule = sym.asClass.companionSymbol.asModule
      mirror.reflectModule(companionModule).instance.asInstanceOf[SoQLValueCompanion]
    }

    companions.map(_.typ).size must equal (companions.size)
  }
}
