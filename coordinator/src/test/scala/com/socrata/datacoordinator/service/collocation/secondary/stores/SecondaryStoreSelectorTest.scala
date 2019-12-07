package com.socrata.datacoordinator.service.collocation.secondary.stores

import com.socrata.datacoordinator.service.collocation.{Cost, WeightedCostOrdering}
import org.scalatest.{FunSuite, Matchers}

class SecondaryStoreSelectorTest extends FunSuite with Matchers {

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

  val stores = Map(store1 -> 100L, store2 -> 100L, store3 -> 100L, store4 -> 100L, store5 -> 100L)
  def secondaryStoreSelector(stores: Map[String, Long],
                             unavailableStores: Set[String] = Set.empty,
                             replicationFactor: Int) = {
    val storesMap = stores.map { case (store, actualFreeSpace) =>
      val freeSpace = if (unavailableStores(store)) 0L else actualFreeSpace
      (store, freeSpace)
    }
    new SecondaryStoreSelector("group_1", storesMap, replicationFactor)
  }

  val selector = secondaryStoreSelector(stores, replicationFactor = 1)
  val selectorUnavailableStore1 = secondaryStoreSelector(stores, Set(store1), replicationFactor = 1)
  val selectorRep0 = secondaryStoreSelector(stores, replicationFactor = 0)
  val selectorRep2 = secondaryStoreSelector(stores, replicationFactor = 2)

  val selectorRep2UnavailableStore1 = secondaryStoreSelector(stores, Set(store1), replicationFactor = 2)
  val selectorRep2UnavailableStore13 = secondaryStoreSelector(stores, Set(store1, store3), replicationFactor = 2)
  val selectorRep2UnavailableStore123 = secondaryStoreSelector(stores, Set(store1, store2, store3), replicationFactor = 2)
  val selectorRep2UnavailableStore1234 = secondaryStoreSelector(stores, Set(store1, store2, store3, store4), replicationFactor = 2)

  val costMapEmpty = Map.empty[Int, Cost]
  val storeMapEmpty = Map.empty[Int, Set[String]]

  test("There should be no key for the max cost in an empty cost map") {
    selector.maxCostKey(costMapEmpty) should be (None)
  }

  test("An available store should always be a valid destination for an empty store map") {
    selector.invalidDestinationStore(store1, storeMapEmpty, costMapEmpty) should be (false)
  }

  test("An unavailable store should always be a valid destination for an empty store map") {
    selectorUnavailableStore1.invalidDestinationStore("store_1", storeMapEmpty, costMapEmpty) should be (false)
  }

  test("Should select replication factor number of stores for empty store and cost maps") {
    val destinations0 = selectorRep0.destinationStores(storeMapEmpty, costMapEmpty)
    destinations0 should have size 0
    stores.keySet.intersect(destinations0) should have size 0

    val destinations1 = selector.destinationStores(storeMapEmpty, costMapEmpty)
    destinations1 should have size 1
    stores.keySet.intersect(destinations1) should have size 1

    val destinations2 = selectorRep2.destinationStores(storeMapEmpty, costMapEmpty)
    destinations2 should have size 2
    stores.keySet.intersect(destinations2) should have size 2
  }

  val costMap = Map(0 -> Cost(4, 40), 1 -> Cost(1, 10), 2 -> Cost(1, 10), 3 -> Cost(2, 20))
  val storeMap = Map(0 -> Set(store1), 1 -> Set(store2), 2 -> Set(store3), 3 -> Set(store3))

  test("There should be a key for the max cost in a cost map") {
    selector.maxCostKey(costMap) should be (Some(0))
  }

  test("An available store with enough space should to be a valid destination for a store map") {
    selector.invalidDestinationStore(store1, storeMap, costMap) should be (false)

    val costMap2 = Map(0 -> Cost(4, 200), 1 -> Cost(1, 10), 2 -> Cost(1, 10), 3 -> Cost(2, 20))
    selector.invalidDestinationStore(store1, storeMap, costMap2) should be (false)
  }

  test("An available store with not enough space should to be an invalid destination for a store map") {
    val costMap = Map(0 -> Cost(4, 40), 1 -> Cost(1, 10), 2 -> Cost(1, 10), 3 -> Cost(2, 90))
    selector.invalidDestinationStore(store1, storeMap, costMap) should be (true)
  }

  test("An unavailable store should be an invalid destination for a store map") {
    selectorUnavailableStore1.invalidDestinationStore(store1, storeMap, costMap) should be (true)
  }

  val store1Map = Map(0 -> Set(store1), 1 -> Set(store1), 2 -> Set(store1), 3 -> Set(store1))

  test("An unavailable store should be a valid destination for a store map always containing that store") {
    selectorUnavailableStore1.invalidDestinationStore(store1, store1Map, costMap) should be (false)
  }

  test("Should select the store for the key with the highest cost") {
    selector.destinationStores(storeMap, costMap) should be (Set(store1))
  }

  test("Should select the store where there is enough free space") {
    // 0, 1 on store1
    // 2, 3 on store3
    val costMap = Map(0 -> Cost(4, 45), 1 -> Cost(1, 45), 2 -> Cost(1, 30), 3 -> Cost(2, 30))
    selector.destinationStores(storeMap, costMap) should be (Set(store3))
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
