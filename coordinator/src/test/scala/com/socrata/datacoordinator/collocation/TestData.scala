package com.socrata.datacoordinator.collocation

import com.socrata.datacoordinator.id.{DatasetId, DatasetInternalName}
import com.socrata.datacoordinator.service.collocation.{CollocationRequest, Cost}

object TestData {
  // data-coordinator (truth) instances
  val alpha = "alpha"
  val bravo = "bravo"
  val charlie = "charlie"

  // secondary store groups
  val storeGroupA = "store_group_a"
  val storeGroupB = "store_group_b"

  val defaultStoreGroups = Set(storeGroupA, storeGroupB)

  // secondary store instances
  val store1A = "store_1_a"
  val store3A = "store_3_a"
  val store5A = "store_5_a"
  val store7A = "store_7_a"

  val storesGroupA = Set(store1A, store3A, store5A, store7A)

  val store2B = "store_2_b"
  val store4B = "store_4_b"
  val store6B = "store_6_b"
  val store8B = "store_8_b"

  val storesGroupB = Set(store2B, store4B, store6B, store8B)

  // datasets
  def internalName(instance: String, datasetId: Long): DatasetInternalName =
    DatasetInternalName(instance, new DatasetId(datasetId))

  val alpha1 = internalName(alpha, 1L)
  val alpha2 = internalName(alpha, 2L)

  val bravo1 = internalName(bravo, 1L)
  val bravo2 = internalName(bravo, 2L)

  val charlie1 = internalName(charlie, 1L)
  val charlie2 = internalName(charlie, 2L)
  val charlie3 = internalName(charlie, 3L)

  val costLimits = Cost(moves = 20, totalSizeBytes = 100L, moveSizeMaxBytes = Some(50L))

  def request(collocations: Seq[(DatasetInternalName, DatasetInternalName)]) =
    CollocationRequest(collocations = collocations, costLimits)

  val requestEmpty = request(Seq.empty)
}
