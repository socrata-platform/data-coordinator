package com.socrata.datacoordinator.truth.metadata

import org.scalatest.MustMatchers
import org.scalatest.FunSuite

class LifecycleStageTest extends FunSuite with MustMatchers {
  test("Both LifecycleStages have the same members") {
    LifecycleStage.values.map(_.name).toSet must equal (com.socrata.datacoordinator.secondary.LifecycleStage.values.map(_.name).toSet)
  }

  test("Local LifecycleStages point to the correct secondary stages") {
    LifecycleStage.values.foreach { v =>
      v.correspondingSecondaryStage.name must equal (v.name)
    }
  }
}
