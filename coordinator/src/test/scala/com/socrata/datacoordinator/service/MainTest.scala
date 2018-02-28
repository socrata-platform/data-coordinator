package com.socrata.datacoordinator.service

import com.socrata.datacoordinator.secondary.config.{SecondaryGroupConfig, StoreConfig}
import com.socrata.datacoordinator.id.DatasetId
import org.scalatest.{FunSuite, MustMatchers}

class MainTest extends FunSuite with MustMatchers {
  private val sg1 = SecondaryGroupConfig(2, Map("pg1" -> StoreConfig(1000, true), "pg2" -> StoreConfig(1000, true), "pg3" -> StoreConfig(1000, true)))
  private val sg2 = SecondaryGroupConfig(2, Map("pg1" -> StoreConfig(1000, true), "pg2" -> StoreConfig(1000, false), "pg3" -> StoreConfig(1000, true)))
  private val sg3 = SecondaryGroupConfig(2, Map("pg1" -> StoreConfig(1000, true), "pg2" -> StoreConfig(1000, false), "pg3" -> StoreConfig(1000, false)))
  private val ds = new DatasetId(1234)

  test("do nothing if already have sufficient") {
    val newSecondaries = Main.secondariesToAdd(sg1, Set("pg1", "pg3"), ds, "g1")
    newSecondaries must have size 0
  }

  test("add if not enough in group") {
    // ensure it has enough from the specific  group, not just any group
    val newSecondaries = Main.secondariesToAdd(sg1, Set("pg1", "pg4", "pgx"), ds, "g1")
    newSecondaries must have size 1
    newSecondaries.intersect(sg1.instances.keySet) must have size 1
  }

  test("add if none in group") {
    val newSecondaries = Main.secondariesToAdd(sg1, Set(), ds, "g1")
    newSecondaries must have size 2
    newSecondaries.intersect(sg1.instances.keySet) must have size 2
  }

  test("add but not to instance not accepting new datasets") {
    val newSecondaries = Main.secondariesToAdd(sg2, Set.empty, ds, "g1")
    newSecondaries must have size 2
    newSecondaries.intersect(sg2.instances.keySet) must have size 2
    newSecondaries.intersect(Set("pg2")) must have size 0
  }

  test("add but not to instance not accepting new datasets fails when there are not enough available instances") {
    val result = intercept[Exception] {
      Main.secondariesToAdd(sg3, Set.empty, ds, "g1")
    }
    result.getMessage must be ("Can't find 2 servers in secondary group g1 to publish to")
  }
}
