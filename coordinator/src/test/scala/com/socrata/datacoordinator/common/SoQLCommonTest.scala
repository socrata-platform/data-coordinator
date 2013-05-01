package com.socrata.datacoordinator.common

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import com.socrata.soql.environment.ColumnName

class SoQLCommonTest extends FunSuite with MustMatchers {
  test("allSystemNames actually names all system names") {
    val allSystemColumnNames = {
      SoQLSystemColumns.getClass.getMethods.filter(_.getReturnType == classOf[ColumnName]).filter(_.getParameterTypes.isEmpty).map { method =>
        method.invoke(SoQLSystemColumns).asInstanceOf[ColumnName]
      }.toSet
    }

    SoQLSystemColumns.allSystemColumnNames must equal (allSystemColumnNames)
  }
}
