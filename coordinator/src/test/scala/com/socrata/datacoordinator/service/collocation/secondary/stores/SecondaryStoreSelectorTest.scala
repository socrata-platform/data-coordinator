package com.socrata.datacoordinator.service.collocation.secondary.stores

import com.socrata.datacoordinator.service.collocation.{Cost, WeightedCostOrdering}
import org.scalatest.{FunSuite, ShouldMatchers}

class SecondaryStoreSelectorTest extends FunSuite with ShouldMatchers {

  implicit val costOrdering: Ordering[Cost] = WeightedCostOrdering(
    movesWeight = 1.0,
    totalSizeBytesWeight = 0.0,
    moveSizeMaxBytesWeight = 0.0
  )

  val store1 = "store_1"
  val store2 = "store_2"
  val store3 = "store_3"
  val store4 = "store_4"
  val store5 = "store_5"

  val stores = Set(store1, store2, store3, store4, store5)

  val selector = new SecondaryStoreSelector("group_1", stores, Set.empty[String], replicationFactor = 1)
  val selectorUnavailableStore1 = new SecondaryStoreSelector("group_1", stores, Set(store1), replicationFactor = 1)

  val selectorRep0 = new SecondaryStoreSelector("group_1", stores, Set.empty[String], replicationFactor = 0)
  val selectorRep2 = new SecondaryStoreSelector("group_1", stores, Set.empty[String], replicationFactor = 2)

  val selectorRep2UnavailableStore1 = new SecondaryStoreSelector("group_1", stores, Set(store1), replicationFactor = 2)
  val selectorRep2UnavailableStore13 = new SecondaryStoreSelector("group_1", stores, Set(store1, store3), replicationFactor = 2)
  val selectorRep2UnavailableStore123 = new SecondaryStoreSelector("group_1", stores, Set(store1, store2, store3), replicationFactor = 2)
  val selectorRep2UnavailableStore1234 = new SecondaryStoreSelector("group_1", stores, Set(store1, store2, store3, store4), replicationFactor = 2)

  val costMapEmpty = Map.empty[Int, Cost]
  val storeMapEmpty = Map.empty[Int, Set[String]]

  test("There should be no key for the max cost in an empty cost map") {
    selector.maxCostKey(costMapEmpty) should be (None)
  }

  test("An available store should always be a valid destination for an empty store map") {
    selector.invalidDestinationStore(store1, storeMapEmpty) should be (false)
  }

  test("An unavailable store should always be a valid destination for an empty store map") {
    selectorUnavailableStore1.invalidDestinationStore("store_1", storeMapEmpty) should be (false)
  }

  test("Should select replication factor number of stores for empty store and cost maps") {
    val destinations0 = selectorRep0.destinationStores(storeMapEmpty, costMapEmpty)
    destinations0 should have size 0
    stores.intersect(destinations0) should have size 0

    val destinations1 = selector.destinationStores(storeMapEmpty, costMapEmpty)
    destinations1 should have size 1
    stores.intersect(destinations1) should have size 1

    val destinations2 = selectorRep2.destinationStores(storeMapEmpty, costMapEmpty)
    destinations2 should have size 2
    stores.intersect(destinations2) should have size 2
  }

  val costMap = Map(0 -> Cost(4, 40), 1 -> Cost(1, 10), 2 -> Cost(1, 10), 3 -> Cost(2, 20))
  val storeMap = Map(0 -> Set(store1), 1 -> Set(store2), 2 -> Set(store3), 3 -> Set(store3))

  test("There should be a key for the max cost in a cost map") {
    selector.maxCostKey(costMap) should be (Some(0))
  }

  test("An available store should always to be a valid destination for a store map") {
    selector.invalidDestinationStore(store1, storeMap) should be (false)
  }

  test("An unavailable store should be an invalid destination for a store map") {
    selectorUnavailableStore1.invalidDestinationStore(store1, storeMap) should be (true)
  }

  val store1Map = Map(0 -> Set(store1), 1 -> Set(store1), 2 -> Set(store1), 3 -> Set(store1))

  test("An unavailable store should be a valid destination for a store map always containing that store") {
    selectorUnavailableStore1.invalidDestinationStore(store1, store1Map) should be (false)
  }

  test("Should select the store for the key with the highest cost") {
    selector.destinationStores(storeMap, costMap) should be (Set(store1))
  }

  test("Should select the available store for the key with the highest cost when preferred stores are unavailable") {
    selectorUnavailableStore1.destinationStores(storeMap, costMap) should be (Set(store3))
  }

  val storeMapRep2 = Map(0 -> Set(store1, store3), 1 -> Set(store1, store2), 2 -> Set(store2, store3), 3 -> Set(store1, store4))

  test("Should select the stores for the key with the highest cost") {
    selectorRep2.destinationStores(storeMapRep2, costMap) should be (Set(store1, store3))
  }

  test("Should select the available stores for the key with the highest cost when preferred stores are unavailable") {
    selectorRep2UnavailableStore1.destinationStores(storeMapRep2, costMap) should be (Set(store3, store4))
  }

  val store1MapRep2 = Map(0 -> Set(store1, store3), 1 -> Set(store1, store2), 2 -> Set(store1, store3), 3 -> Set(store1, store4))

  test("Should select an unavailable store if all keys map to that store") {
    selectorRep2UnavailableStore1.destinationStores(store1MapRep2, costMap) should be (Set(store1, store3))
  }

  test("Should select an unavailable store if all keys map to that store and the available store with the highest cost") {
    selectorRep2UnavailableStore13.destinationStores(store1MapRep2, costMap) should be (Set(store1, store4))
  }

  val storeMapRep2Unavailable = Map(0 -> Set(store1, store3), 1 -> Set(store1, store3), 2 -> Set(store1, store2), 3 -> Set(store2, store3))

  test("Should select random available stores if all stores are unavailable in store map") {
    selectorRep2UnavailableStore123.destinationStores(storeMapRep2Unavailable, costMap) should be (Set(store4, store5))
  }

  test("Should fail if there are not enough available stores left") {
    val exception = intercept[NotEnoughInstancesInSecondaryGroup] {
      selectorRep2UnavailableStore1234.destinationStores(storeMapRep2Unavailable, costMap) should be(Set(store4, store5))
    }

    exception should be (NotEnoughInstancesInSecondaryGroup("group_1", 2))
  }
}
