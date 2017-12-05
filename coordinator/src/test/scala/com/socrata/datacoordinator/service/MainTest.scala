package com.socrata.datacoordinator.service

import com.socrata.datacoordinator.secondary.config.SecondaryGroupConfig
import com.socrata.datacoordinator.id.DatasetId
import org.scalatest.{MustMatchers, FunSuite}

class MainTest extends FunSuite with MustMatchers {
  private val sg1 = SecondaryGroupConfig(2, Set("pg1", "pg2", "pg3"), None)
  private val ds = new DatasetId(1234)

  test("do nothing if already have sufficient") {
    val newSecondaries = Main.secondariesToAdd(sg1, Set("pg1", "pg3"), ds, "g1")
    newSecondaries must have size 0
  }

  test("add if not enough in group") {
    // ensure it has enough from the specific  group, not just any group
    val newSecondaries = Main.secondariesToAdd(sg1, Set("pg1", "pg4", "pgx"), ds, "g1")
    newSecondaries must have size 1
    newSecondaries.intersect(sg1.instances) must have size 1
  }

  test("add if none in group") {
    val newSecondaries = Main.secondariesToAdd(sg1, Set(), ds, "g1")
    newSecondaries must have size 2
    newSecondaries.intersect(sg1.instances) must have size 2
  }

  test("add but not to full instance") {
    // ensure it has enough from the specific  group, but not add to a full instance
    val newSecondaries = Main.secondariesToAdd(sg1.copy(fullInstances = Some(Set("pg2"))), Set.empty, ds, "g1")
    newSecondaries must have size 2
    newSecondaries.intersect(sg1.instances) must have size 2
    newSecondaries.intersect(Set("pg2")) must have size 0
  }
}
