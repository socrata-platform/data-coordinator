package com.socrata.datacoordinator.service.collocation

import java.util.UUID

import com.socrata.datacoordinator.common.collocation.{CollocationLock, CollocationLockTimeout}
import com.socrata.datacoordinator.id.{DatasetId, DatasetInternalName}
import com.socrata.datacoordinator.resources.SecondariesOfDatasetResult
import com.socrata.datacoordinator.resources.collocation.{CollocatedDatasetsResult, SecondaryMoveJobsResult}
import com.socrata.datacoordinator.secondary.config.SecondaryGroupConfig
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration.FiniteDuration

class CoordinatedCollocatorTest extends FunSuite with Matchers with MockFactory {
  class CollocationManifest {
    private val manifest = collection.mutable.Set.empty[(DatasetInternalName, DatasetInternalName)]

    def add(collocations: Seq[(DatasetInternalName, DatasetInternalName)]): Unit = {
      collocations.foreach(collocation => manifest.add(collocation))
    }

    def get: Set[(DatasetInternalName, DatasetInternalName)] = manifest.toSet
  }

  val alpha = "alpha"
  val bravo = "bravo"
  val charlie = "charlie"

  def expectNoMockCoordinatorCalls(mock: Coordinator): Unit = {
    (mock.rollbackSecondaryMoveJob _).expects(*, *, *, *).never
    (mock.ensureSecondaryMoveJob _).expects(*, *, *).never
    (mock.collocatedDatasetsOnInstance _).expects(*, *).never
    (mock.secondariesOfDataset _).expects(*).never
    (mock.secondaryGroups _).expects().never
    (mock.secondaryMoveJobs _).expects(*, *).never
  }

  def expectCollocatedDatasetsOnInstance(mock: Coordinator,
                                         andNoOtherCalls: Boolean,
                                         params: (String, Set[DatasetInternalName],  Set[DatasetInternalName])*): Unit = {
    params.foreach { case (instance, datasets, result) =>
      (mock.collocatedDatasetsOnInstance _).expects(instance, datasets).once.returns(collocatedDatasets(result))
    }

    if (andNoOtherCalls) {
      (mock.rollbackSecondaryMoveJob _).expects(*, *, *, *).never
      (mock.ensureSecondaryMoveJob _).expects(*, *, *).never
      (mock.secondariesOfDataset _).expects(*).never
      (mock.secondaryGroups _).expects().never
      (mock.secondaryMoveJobs _).expects(*, *).never
    }
  }

  def expectNoMockCollocationLockCalls(mock: CollocationLock): Unit = {
    (mock.release _).expects().never
    (mock.acquire _).expects(*).never
  }

  def withMocks[T](defaultStoreGroups: Set[String],
                   coordinatorExpectations: Coordinator => Unit = expectNoMockCoordinatorCalls,
                   collocationLockExpectations: CollocationLock => Unit = expectNoMockCollocationLockCalls)
                  (f: (Collocator, CollocationManifest) => T): T = {
    val manifest = new CollocationManifest

    val mockCoordinator: Coordinator = mock[Coordinator]
    val mockCollocationLock: CollocationLock = mock[CollocationLock]

    val collocator = new CoordinatedCollocator(
      collocationGroup = Set(alpha, bravo, charlie),
      defaultStoreGroups = defaultStoreGroups,
      coordinator = mockCoordinator,
      addCollocations = manifest.add,
      lock = mockCollocationLock,
      lockTimeoutMillis = FiniteDuration(10, "seconds").toMillis
    )

    coordinatorExpectations(mockCoordinator)
    collocationLockExpectations(mockCollocationLock)

    f(collocator, manifest)
  }

  val storeGroupA = "store_group_a"
  val storeGroupB = "store_group_b"

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

  def groupConfig(numReplicas: Int,
                  instances: Set[String],
                  instancesNotAcceptingNewDatasets: Option[Set[String]] = None) =
    SecondaryGroupConfig(numReplicas, instances, instancesNotAcceptingNewDatasets)

  val defaultStoreGroups = Set(storeGroupA, storeGroupB)

  def internalName(instance: String, datasetId: Long): DatasetInternalName =
    DatasetInternalName(instance, new DatasetId(datasetId))

  val alpha1 = internalName(alpha, 1L)
  val alpha2 = internalName(alpha, 2L)

  val bravo1 = internalName(bravo, 1L)
  val bravo2 = internalName(bravo, 2L)

  val charlie1 = internalName(charlie, 1L)
  val charlie2 = internalName(charlie, 2L)
  val charlie3 = internalName(charlie, 3L)

  val datasetsEmpty = Set.empty[DatasetInternalName]

  def collocatedDatasets(datasets: Set[DatasetInternalName]) = Right(CollocatedDatasetsResult(datasets))

  val collocatedDatasetsEmpty = collocatedDatasets(datasetsEmpty)

  def expectCDOIDatasetsWithNoCollocations(coordinator: Coordinator,
                                           datasets: Set[DatasetInternalName],
                                           andNoOtherCalls: Boolean = false): Unit = {
    expectCollocatedDatasetsOnInstance(coordinator, andNoOtherCalls,
      (alpha, datasets, datasets),
      (bravo, datasets, datasets),
      (charlie, datasets, datasets)
    )
  }

  def expectCDOIAlpha1WithCollocationsSimple(coordinator: Coordinator, andNoOtherCalls: Boolean = false): Unit = {
    // alpha
    // -----------------
    // alpha.1 | bravo.1
    //
    // bravo
    // -----------------
    //
    // charlie
    // -----------------
    //
    expectCollocatedDatasetsOnInstance(coordinator, andNoOtherCalls,
      (alpha,   Set(alpha1), Set(alpha1, bravo1)),
      (bravo,   Set(alpha1), Set(alpha1)),
      (charlie, Set(alpha1), Set(alpha1)),

      (alpha,   Set(bravo1), Set(alpha1, bravo1)),
      (bravo,   Set(bravo1), Set(bravo1)),
      (charlie, Set(bravo1), Set(bravo1))
    )
  }

  def expectCDOIAlpha1WithCollocationsComplex(coordinator: Coordinator, andNoOtherCalls: Boolean = false): Unit = {
    // alpha
    // -----------------
    // alpha.1 | bravo.1
    //
    // bravo
    // -----------------
    // bravo.1 | alpha.2
    // bravo.1 | charlie.1
    //
    // charlie
    // -----------------
    // charlie.1 | alpha.2
    // charlie.2 | alpha.1
    //
    expectCollocatedDatasetsOnInstance(coordinator, andNoOtherCalls,
      (alpha,   Set(alpha1), Set(alpha1, bravo1)),
      (bravo,   Set(alpha1), Set(alpha1)),
      (charlie, Set(alpha1), Set(alpha1, charlie2)),
      // new: bravo1, charlie2

      (alpha,   Set(bravo1, charlie2), Set(alpha1, bravo1, charlie2)),
      (bravo,   Set(bravo1, charlie2), Set(bravo1, alpha2, charlie1, charlie2)),
      (charlie, Set(bravo1, charlie2), Set(alpha1, charlie2)),
      // new: alpha2, charlie1

      (alpha,   Set(alpha2, charlie1), Set(alpha2, charlie1)),
      (bravo,   Set(alpha2, charlie1), Set(bravo1, alpha2, charlie1)),
      (charlie, Set(alpha2, charlie1), Set(alpha2, charlie1))
    )
  }

  def expectCDOIBravo1WithCollocationsComplex(coordinator: Coordinator, andNoOtherCalls: Boolean = false): Unit = {
    // alpha
    // -----------------
    // alpha.1 | bravo.1
    //
    // bravo
    // -----------------
    // bravo.1 | alpha.2
    // bravo.1 | charlie.1
    //
    // charlie
    // -----------------
    // charlie.1 | alpha.2
    // charlie.2 | alpha.1
    //
    expectCollocatedDatasetsOnInstance(coordinator, andNoOtherCalls,
      (alpha,   Set(bravo1), Set(alpha1, bravo1)),
      (bravo,   Set(bravo1), Set(alpha2, bravo1, charlie1)),
      (charlie, Set(bravo1), Set(bravo1)),
      // new: alpha1, alpha2, charlie1

      (alpha,   Set(alpha1, alpha2, charlie1), Set(alpha1, alpha2, bravo1, charlie1)),
      (bravo,   Set(alpha1, alpha2, charlie1), Set(alpha1, alpha2, bravo1, charlie1)),
      (charlie, Set(alpha1, alpha2, charlie1), Set(alpha1, alpha2, charlie1, charlie2)),
      // new: charlie2

      (alpha,   Set(charlie2), Set(charlie2)),
      (bravo,   Set(charlie2), Set(charlie2)),
      (charlie, Set(charlie2), Set(alpha1, charlie2))
    )
  }

  def expectCDOIBravo2WithCollocationsSimple(coordinator: Coordinator, andNoOtherCalls: Boolean = false): Unit = {
    // alpha
    // -----------------
    //
    // bravo
    // -----------------
    // bravo.2 | charlie.3
    //
    // charlie
    // -----------------
    //
    expectCollocatedDatasetsOnInstance(coordinator, andNoOtherCalls,
      (alpha,   Set(bravo2), Set(bravo2)),
      (bravo,   Set(bravo2), Set(bravo2, charlie3)),
      (charlie, Set(bravo2), Set(bravo2)),

      (alpha,   Set(charlie3), Set(charlie3)),
      (bravo,   Set(charlie3), Set(bravo2, charlie3)),
      (charlie, Set(charlie3), Set(charlie3))
    )
  }

  // tests for collocatedDatasets(datasets: Set[DatasetInternalName]): Either[RequestError, CollocatedDatasetsResult]
  test("collocatedDatasets should return the empty set when given the empty set") {
    withMocks(defaultStoreGroups, { coordinator =>
      expectCDOIDatasetsWithNoCollocations(coordinator, datasetsEmpty, andNoOtherCalls = true)
    }) { case (collocator, _) =>
      collocator.collocatedDatasets(datasetsEmpty) should be (collocatedDatasets(datasetsEmpty))
    }
  }

  test("collocatedDatasets for a dataset not collocated with any other should return just the dataset") {
    val datasets = Set(alpha1)

    withMocks(defaultStoreGroups, { coordinator =>
      expectCDOIDatasetsWithNoCollocations(coordinator, datasets, andNoOtherCalls = true)
    }) { case (collocator, _) =>
        collocator.collocatedDatasets(datasets) should be (collocatedDatasets(datasets))
    }
  }

  test("collocatedDatasets for a dataset collocated with one other dataset should return the pair") {
    withMocks(defaultStoreGroups, { coordinator =>
      expectCDOIAlpha1WithCollocationsSimple(coordinator, andNoOtherCalls = true)
    }) { case (collocator, _) =>
      collocator.collocatedDatasets(Set(alpha1)) should be (collocatedDatasets(Set(alpha1, bravo1)))
    }
  }

  test("collocatedDatasets for a dataset should be able to return its full collocated group of datasets") {
    withMocks(defaultStoreGroups, { coordinator =>
      expectCDOIAlpha1WithCollocationsComplex(coordinator, andNoOtherCalls = true)
    }) { case (collocator, _) =>
      collocator.collocatedDatasets(Set(alpha1)) should be (collocatedDatasets(Set(alpha1, alpha2, bravo1, charlie1, charlie2)))
    }
  }

  def request(collocations: Seq[(DatasetInternalName, DatasetInternalName)]) = CollocationRequest(collocations)

  val requestEmpty = request(Seq.empty)

  val resultEmpty = Right(CollocationResult.canonicalEmpty)
  val resultCompleted = Right(CollocationResult.canonicalEmpty.copy(status = Completed, message = Completed.message))

  val moveJobResultEmpty = Right(SecondaryMoveJobsResult(Seq.empty))

  def secondaries(instances: String*) = {
    val s = instances.map { instance => (instance, 0L) }.toMap
    Right(Some(SecondariesOfDatasetResult(0L, 0L, Some(0L), Some(0L), s, Set.empty[String], Map.empty)))
  }

  // test for defaultStoreGroups: Set[String]
  test("defaultStoreGroups should return the value CoordinatedCollocator was created with") {
    withMocks(defaultStoreGroups) { case (collocator, _) =>
        collocator.defaultStoreGroups should be (defaultStoreGroups)
    }
  }

  // tests for explainCollocation(storeGroup: String, request: CollocationRequest): Either[ErrorResult, CollocationResult]
  test("explainCollocation should return the canonical empty result for an empty request") {
    withMocks(Set(storeGroupA), { coordinator =>
      (coordinator.secondaryGroups _).expects().returns(Map(storeGroupA -> groupConfig(2, storesGroupA)))
    }) { case (collocator, _) =>
      collocator.explainCollocation(storeGroupA, requestEmpty) should be (resultEmpty)
    }
  }

  test("explainCollocation for two non-collocated datasets with no overlapping stores should move one dataset twice") {
    withMocks(Set(storeGroupA), { coordinator =>
      (coordinator.secondaryGroups _).expects().returns(Map(storeGroupA -> groupConfig(2, storesGroupA)))

      (coordinator.secondaryMoveJobs _).expects(storeGroupA, alpha1).once.returns(moveJobResultEmpty)
      (coordinator.secondariesOfDataset _).expects(alpha1).once.returns(secondaries(store1A, store3A))

      (coordinator.secondaryMoveJobs _).expects(storeGroupA, bravo1).once.returns(moveJobResultEmpty)
      (coordinator.secondariesOfDataset _).expects(bravo1).once.returns(secondaries(store5A, store7A))

      expectCDOIDatasetsWithNoCollocations(coordinator, Set(alpha1)) // group for alpha.1
      expectCDOIDatasetsWithNoCollocations(coordinator, Set(bravo1)) // group for bravo.1
    }) { case (collocator, _) =>
      val result = collocator.explainCollocation(storeGroupA, request(Seq((alpha1, bravo1)))).right.get
      result.id should be (None)
      result.status should be (Approved)
      result.message should be (Approved.message)
      result.cost should be (Cost(2))

      result.moves.size should be (2)
      result.moves.map(_.datasetInternalName).toSet.size should be (1)
      result.moves.map(_.storeIdTo).size should be (2)
    }
  }

  test("explainCollocation for two non-collocated datasets with one overlapping store should move one dataset once") {
    withMocks(Set(storeGroupA), { coordinator =>
      (coordinator.secondaryGroups _).expects().returns(Map(storeGroupA -> groupConfig(2, storesGroupA)))

      (coordinator.secondaryMoveJobs _).expects(storeGroupA, alpha1).once.returns(moveJobResultEmpty)
      (coordinator.secondariesOfDataset _).expects(alpha1).once.returns(secondaries(store1A, store3A))

      (coordinator.secondaryMoveJobs _).expects(storeGroupA, bravo1).once.returns(moveJobResultEmpty)
      (coordinator.secondariesOfDataset _).expects(bravo1).once.returns(secondaries(store3A, store5A))

      expectCDOIDatasetsWithNoCollocations(coordinator, Set(alpha1)) // group for alpha.1
      expectCDOIDatasetsWithNoCollocations(coordinator, Set(bravo1)) // group for bravo.1
    }) { case (collocator, _) =>
      val result = collocator.explainCollocation(storeGroupA, request(Seq((alpha1, bravo1)))).right.get
      result.id should be (None)
      result.status should be (Approved)
      result.message should be (Approved.message)
      result.cost should be (Cost(1))

      result.moves.size should be (1)
      val move = result.moves.head
      Set(move.storeIdFrom, move.storeIdTo) should be (Set(store1A, store5A))
    }
  }

  test("explainCollocation for non-collocated datasets on the same stores should generate no moves") {
    withMocks(Set(storeGroupA), { coordinator =>
      (coordinator.secondaryGroups _).expects().returns(Map(storeGroupA -> groupConfig(2, storesGroupA)))

      (coordinator.secondaryMoveJobs _).expects(storeGroupA, alpha1).once.returns(moveJobResultEmpty)
      (coordinator.secondariesOfDataset _).expects(alpha1).once.returns(secondaries(store1A, store3A))

      (coordinator.secondaryMoveJobs _).expects(storeGroupA, bravo1).once.returns(moveJobResultEmpty)
      (coordinator.secondariesOfDataset _).expects(bravo1).once.returns(secondaries(store1A, store3A))

      expectCDOIDatasetsWithNoCollocations(coordinator, Set(alpha1)) // group for alpha.1
      expectCDOIDatasetsWithNoCollocations(coordinator, Set(bravo1)) // group for bravo.1
    }) { case (collocator, _) =>
      collocator.explainCollocation(storeGroupA, request(Seq((alpha1, bravo1)))) should be (resultEmpty)
    }
  }

  test("explainCollocation for already collocated datasets should return a result with complted status") {
    withMocks(Set(storeGroupA), { coordinator =>
      (coordinator.secondaryGroups _).expects().returns(Map(storeGroupA -> groupConfig(2, storesGroupA)))

      (coordinator.secondaryMoveJobs _).expects(storeGroupA, alpha1).once.returns(moveJobResultEmpty)
      (coordinator.secondariesOfDataset _).expects(alpha1).once.returns(secondaries(store1A, store3A))

      (coordinator.secondaryMoveJobs _).expects(storeGroupA, bravo1).once.returns(moveJobResultEmpty)
      (coordinator.secondariesOfDataset _).expects(bravo1).once.returns(secondaries(store1A, store3A))

      expectCDOIAlpha1WithCollocationsComplex(coordinator) // group for alpha.1
      expectCDOIBravo1WithCollocationsComplex(coordinator) // group for bravo.1
    }) { case (collocator, _) =>
      collocator.explainCollocation(storeGroupA, request(Seq((alpha1, bravo1)))) should be (resultCompleted)
    }
  }

  // collocate: alpha.1 (store1A, store3A) with bravo.2 (store5A, store7A)
  //
  // moves:
  //   bravo.2: store5A, store7A -> store1A, store3A
  //
  test("explainCollocation for a collocated dataset and a non-collocated dataset should move the non-collocated dataset twice") {
    withMocks(Set(storeGroupA), { coordinator =>
      (coordinator.secondaryGroups _).expects().returns(Map(storeGroupA -> groupConfig(2, storesGroupA)))

      (coordinator.secondaryMoveJobs _).expects(storeGroupA, alpha1).once.returns(moveJobResultEmpty)
      (coordinator.secondariesOfDataset _).expects(alpha1).once.returns(secondaries(store1A, store3A))

      (coordinator.secondaryMoveJobs _).expects(storeGroupA, bravo2).once.returns(moveJobResultEmpty)
      (coordinator.secondariesOfDataset _).expects(bravo2).once.returns(secondaries(store5A, store7A))

      expectCDOIAlpha1WithCollocationsComplex(coordinator) // group for alpha.1
      expectCDOIDatasetsWithNoCollocations(coordinator, Set(bravo2)) // group for bravo.2
    }) { case (collocator, _) =>
      val result = collocator.explainCollocation(storeGroupA, request(Seq((alpha1, bravo2)))).right.get
      result.id should be (None)
      result.status should be (Approved)
      result.message should be (Approved.message)
      result.cost should be (Cost(2))

      result.moves.size should be (2)
      result.moves.map(_.datasetInternalName).toSet should be (Set(bravo2))
      result.moves.map(_.storeIdFrom).toSet should be (Set(store5A, store7A))
      result.moves.map(_.storeIdTo).toSet should be (Set(store1A, store3A))
    }
  }

  // collocate: alpha.1 (store1A, store3A) with bravo.2 (store5A, store7A)
  //
  // moves:
  //   bravo.2:   store5A, store7A -> store1A, store3A
  //   charlie.3: store5A, store7A -> store1A, store3A
  //
  test("explainCollocation for two disjointly collocated datasets should move the dataset with the smaller collocated group") {
    withMocks(Set(storeGroupA), { coordinator =>
      (coordinator.secondaryGroups _).expects().returns(Map(storeGroupA -> groupConfig(2, storesGroupA)))

      (coordinator.secondaryMoveJobs _).expects(storeGroupA, alpha1).once.returns(moveJobResultEmpty)
      (coordinator.secondariesOfDataset _).expects(alpha1).once.returns(secondaries(store1A, store3A))

      (coordinator.secondaryMoveJobs _).expects(storeGroupA, bravo2).once.returns(moveJobResultEmpty)
      (coordinator.secondariesOfDataset _).expects(bravo2).once.returns(secondaries(store5A, store7A))

      expectCDOIAlpha1WithCollocationsComplex(coordinator) // group for alpha.1
      expectCDOIBravo2WithCollocationsSimple(coordinator) // group for bravo.2
    }) { case (collocator, _) =>
      val result = collocator.explainCollocation(storeGroupA, request(Seq((alpha1, bravo2)))).right.get
      result.id should be (None)
      result.status should be (Approved)
      result.message should be (Approved.message)
      result.cost should be (Cost(4))

      result.moves.size should be (4)
      result.moves.map(_.datasetInternalName).toSet should be (Set(bravo2, charlie3))
      result.moves.map(_.storeIdFrom).toSet should be (Set(store5A, store7A))
      result.moves.map(_.storeIdTo).toSet should be (Set(store1A, store3A))
    }
  }

  // def initiateCollocation(jobId: UUID, storeGroup: String, request: CollocationRequest): (Either[ErrorResult, CollocationResult], Seq[(Move, Boolean)])
  // tests for saveCollocation(request: CollocationRequest): Unit
  test("saveCollocation for an empty request should save nothing to the manifest") {
    withMocks(defaultStoreGroups) { (collocator, manifest) =>
      collocator.saveCollocation(requestEmpty)

      manifest.get should be (Set.empty)
    }
  }

  test("saveCollocation for a single pair should save that pair to the manifest") {
    withMocks(defaultStoreGroups) { (collocator, manifest) =>
      collocator.saveCollocation(request(Seq((alpha1, bravo1))))

      manifest.get should be (Set((alpha1, bravo1)))
    }
  }

  test("saveCollocation for multiple pairs should save those pairs to the manifest") {
    withMocks(defaultStoreGroups) { (collocator, manifest) =>
      val collocations = Seq(
        (alpha1, bravo1),
        (bravo1, charlie2),
        (alpha1, charlie2)
      )
      collocator.saveCollocation(request(collocations))

      manifest.get should be (collocations.toSet)
    }
  }

  // tests for beginCollocation(): Unit
  test("beginCollocation should acquire the collocation lock") {
    withMocks(defaultStoreGroups, expectNoMockCoordinatorCalls, { lock =>
      (lock.acquire _).expects(10000L).once.returns(true)

    }) { (collocator, _) =>
      collocator.beginCollocation()
    }
  }

  test("beginCollocation should throw a CollocationLockTimeout exception if it cannot acquire the collocation lock") {
    withMocks(defaultStoreGroups, expectNoMockCoordinatorCalls, { lock =>
      (lock.acquire _).expects(10000L).once.returns(false)

    }) { (collocator, _) =>
      val result = intercept[CollocationLockTimeout] {
        collocator.beginCollocation()
      }

      result.millis should be (10000L)
    }
  }

  // tests for commitCollocation(): Unit
  test("commitCollocation should release the collocation lock") {
    withMocks(defaultStoreGroups, expectNoMockCoordinatorCalls, { lock =>
      (lock.release _).expects().once

    }) { (collocator, _) =>
      collocator.commitCollocation()
    }
  }

  // tests for rollbackCollocation(jobId: UUID, moves: Seq[(Move, Boolean)]): Option[ErrorResult]
  test("rollbackCollocation for no moves should do nothing") {
    withMocks(defaultStoreGroups){ (collocator, _) =>
      val jobId = UUID.randomUUID()
      collocator.rollbackCollocation(jobId, Seq.empty) should be (None)
    }
  }

  val moveBravo2To1A = Move(bravo2, store7A, store1A)
  val moveBravo2To3A = Move(bravo2, store5A, store3A)

  val moveCharlie3To1A = Move(charlie3, store7A, store1A)
  val moveCharlie3To3A = Move(charlie3, store5A, store3A)

  val moveBravo2AndCharlie3 = Seq(
    (moveBravo2To1A, true),
    (moveBravo2To3A, false),
    (moveCharlie3To1A, true),
    (moveCharlie3To3A, true)
  )

  test("rollbackCollocation for a set of moves should rollback each move job on the correct instance") {
    val jobId = UUID.randomUUID()

    withMocks(defaultStoreGroups, { coordinator =>
      (coordinator.rollbackSecondaryMoveJob _).expects(bravo, jobId, moveBravo2To1A, true).returns(None)
      (coordinator.rollbackSecondaryMoveJob _).expects(bravo, jobId, moveBravo2To3A, false).returns(None)
      (coordinator.rollbackSecondaryMoveJob _).expects(charlie, jobId, moveCharlie3To1A, true).returns(None)
      (coordinator.rollbackSecondaryMoveJob _).expects(charlie, jobId, moveCharlie3To3A, true).returns(None)

    }) { (collocator, manifest) =>
      collocator.rollbackCollocation(jobId, moveBravo2AndCharlie3) should be (None)
    }
  }

  test("rollbackCollocation for a set of moves should rollback each move job on the correct instance handling errors") {
    val jobId = UUID.randomUUID()

    withMocks(defaultStoreGroups, { coordinator =>
      (coordinator.rollbackSecondaryMoveJob _).expects(bravo, jobId, moveBravo2To1A, true).returns(None)
      (coordinator.rollbackSecondaryMoveJob _).expects(bravo, jobId, moveBravo2To3A, false).returns(Some(DatasetNotFound(bravo2)))
      (coordinator.rollbackSecondaryMoveJob _).expects(charlie, jobId, moveCharlie3To1A, true).returns(None)
      (coordinator.rollbackSecondaryMoveJob _).expects(charlie, jobId, moveCharlie3To3A, true).returns(None)

    }) { (collocator, manifest) =>
      collocator.rollbackCollocation(jobId, moveBravo2AndCharlie3) should be (Some(DatasetNotFound(bravo2)))
    }
  }
}
