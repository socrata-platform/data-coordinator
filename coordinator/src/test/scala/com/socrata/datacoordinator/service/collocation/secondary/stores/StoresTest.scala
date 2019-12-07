package com.socrata.datacoordinator.service.collocation.secondary.stores

import org.scalatest.{FunSuite, Matchers}

class StoresTest extends FunSuite with Matchers {

  def testRep(name: String, count: Int = 10)(f: => Unit): Unit = {
    test(name) {
      1 to count foreach { _ => f }
    }
  }

  val storesEmpty = Set.empty[String]
  val storesSingleton = Set("store_1")
  val storesMultiple = Set("store_1", "store_2", "store_3")

  testRep("For the empty set of stores, there should be no random store") {
    randomStore(storesEmpty) should be (None)
  }

  testRep("For the empty set of stores, there should be no random stores for count more than 0") {
    randomStores(storesEmpty, -2) should be (Some(storesEmpty))
    randomStores(storesEmpty, 0) should be (Some(storesEmpty))
    randomStores(storesEmpty, 1) should be (None)
  }

  testRep("For a singleton set of stores, there should be one random store") {
    randomStore(storesSingleton) should be (Some("store_1"))
  }

  testRep("For a singleton set of stores, it should have no random stores for count more than 1") {
    randomStores(storesSingleton, -2) should be (Some(storesEmpty))
    randomStores(storesSingleton, 0) should be (Some(storesEmpty))
    randomStores(storesSingleton, 1) should be (Some(storesSingleton))
    randomStores(storesSingleton, 2) should be (None)
  }

  testRep("For a set of stores, it should have no random stores for count more than its size") {
    randomStores(storesMultiple, -2) should be (Some(storesEmpty))
    randomStores(storesMultiple, 0) should be (Some(storesEmpty))

    val stores1 = randomStores(storesMultiple, 1).get
    stores1 should have size 1
    storesMultiple.intersect(stores1) should have size 1

    val stores2 = randomStores(storesMultiple, 2).get
    stores2 should have size 2
    storesMultiple.intersect(stores2) should have size 2

    val stores3 = randomStores(storesMultiple, 3).get
    stores3 should have size 3
    storesMultiple.intersect(stores3) should have size 3

    randomStores(storesMultiple, 4) should be (None)
  }
}
