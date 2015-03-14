package com.socrata.datacoordinator.common

import org.scalatest.FunSuite
import org.scalatest.MustMatchers
import com.socrata.soql.environment.ColumnName
import com.socrata.datacoordinator.id.UserColumnId

class SoQLCommonTest extends FunSuite with MustMatchers {
  test("allSystemNames actually names all system names") {
    val allSystemColumnIds = {
      // This is a little fragile because UserColumnId is a value class.
      // Fortunately, the test should fail noisily if anything changes.
      SoQLSystemColumns.getClass.getDeclaredMethods.filter(_.getReturnType == classOf[String]).filter(_.getParameterTypes.isEmpty).map { method =>
        new UserColumnId(method.invoke(SoQLSystemColumns).asInstanceOf[String])
      }.toSet
    }

    SoQLSystemColumns.allSystemColumnIds.toSet must equal (allSystemColumnIds)
  }
}
