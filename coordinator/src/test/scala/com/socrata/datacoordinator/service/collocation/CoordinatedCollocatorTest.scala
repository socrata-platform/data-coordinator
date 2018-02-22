package com.socrata.datacoordinator.service.collocation

import java.util.UUID

import com.socrata.datacoordinator.common.collocation.{CollocationLock, CollocationLockTimeout}
import com.socrata.datacoordinator.id.{DatasetId, DatasetInternalName}
import com.socrata.datacoordinator.resources.SecondariesOfDatasetResult
import com.socrata.datacoordinator.resources.collocation.{CollocatedDatasetsResult, SecondaryMoveJobsResult}
import com.socrata.datacoordinator.secondary.SecondaryMetric
import com.socrata.datacoordinator.secondary.config.SecondaryGroupConfig
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration.FiniteDuration

class CoordinatedCollocatorTest extends FunSuite with Matchers with MockFactory {

  implicit val costOrdering: Ordering[Cost] = WeightedCostOrdering(
    movesWeight = 1.0,
    totalSizeBytesWeight = 0.0,
    moveSizeMaxBytesWeight = 0.0
  )

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
    (mock.secondaryGroupConfigs _).expects().never
    (mock.secondaryMoveJobs _: (String, UUID) => Either[RequestError, SecondaryMoveJobsResult]).expects(*, *).never
    (mock.secondaryMoveJobs _: (String, DatasetInternalName) => Either[ErrorResult, SecondaryMoveJobsResult]).expects(*, *).never
    (mock.secondaryMetrics _: (String, DatasetInternalName) => Either[ErrorResult, Option[SecondaryMetric]]).expects(*, *).never
  }

  def expectNoMockMetricCalls(mock: Metric): Unit = {
    (mock.datasetMaxCost _).expects(*, *).never
  }

  def expectCollocatedDatasetsOnInstance(mock: Coordinator,
                                         andNoOtherCalls: Boolean,
                                         params: (String, Set[DatasetInternalName],  Set[DatasetInternalName])*): Unit = {
    params.foreach { case (instance, datasets, result) =>
      (mock.collocatedDatasetsOnInstance _).expects(instance, datasets).once.returns(collocatedDatasets(result))
    }

    if (andNoOtherCalls) {
      expectNoMockCoordinatorCalls(mock)
    }
  }

  def expectNoMockCollocationLockCalls(mock: CollocationLock): Unit = {
    (mock.release _).expects().never
    (mock.acquire _).expects(*).never
  }

  def withMocks[T](defaultStoreGroups: Set[String],
                   coordinatorExpectations: Coordinator => Unit = expectNoMockCoordinatorCalls,
                   metricExpectations: Metric => Unit = expectNoMockMetricCalls,
                   collocationLockExpectations: CollocationLock => Unit = expectNoMockCollocationLockCalls)
                  (f: (Collocator, CollocationManifest) => T): T = {
    val manifest = new CollocationManifest

    val mockCoordinator: Coordinator = mock[Coordinator]
    val mockMetric: Metric = mock[Metric]
    val mockCollocationLock: CollocationLock = mock[CollocationLock]

    val collocator = new CoordinatedCollocator(
      collocationGroup = Set(alpha, bravo, charlie),
      coordinator = mockCoordinator,
      metric = mockMetric,
      addCollocations = manifest.add,
      lock = mockCollocationLock,
      lockTimeoutMillis = FiniteDuration(10, "seconds").toMillis
    )

    coordinatorExpectations(mockCoordinator)
    metricExpectations(mockMetric)
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

  def expectCDOIDatasetsWithNoCollocations(datasets: Set[DatasetInternalName],
                                           andNoOtherCalls: Boolean = false)(implicit coordinator: Coordinator): Unit = {
    expectCollocatedDatasetsOnInstance(coordinator, andNoOtherCalls,
      (alpha, datasets, datasets),
      (bravo, datasets, datasets),
      (charlie, datasets, datasets)
    )
  }

  def expectCDOIAlpha1WithCollocationsSimple(andNoOtherCalls: Boolean = false)(implicit coordinator: Coordinator): Unit = {
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

  def expectCDOIAlpha1WithCollocationsComplex(andNoOtherCalls: Boolean = false)(implicit coordinator: Coordinator): Unit = {
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

  def expectCDOIBravo1WithCollocationsComplex(andNoOtherCalls: Boolean = false)(implicit coordinator: Coordinator): Unit = {
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

  def expectCDOIBravo2WithCollocationsSimple(andNoOtherCalls: Boolean = false)(implicit coordinator: Coordinator): Unit = {
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
    withMocks(defaultStoreGroups, { implicit coordinator =>
      expectCDOIDatasetsWithNoCollocations(datasetsEmpty, andNoOtherCalls = true)
    }) { case (collocator, _) =>
      collocator.collocatedDatasets(datasetsEmpty) should be (collocatedDatasets(datasetsEmpty))
    }
  }

  test("collocatedDatasets for a dataset not collocated with any other should return just the dataset") {
    val datasets = Set(alpha1)

    withMocks(defaultStoreGroups, { implicit coordinator =>
      expectCDOIDatasetsWithNoCollocations( datasets, andNoOtherCalls = true)
    }) { case (collocator, _) =>
        collocator.collocatedDatasets(datasets) should be (collocatedDatasets(datasets))
    }
  }

  test("collocatedDatasets for a dataset collocated with one other dataset should return the pair") {
    withMocks(defaultStoreGroups, { implicit coordinator =>
      expectCDOIAlpha1WithCollocationsSimple(andNoOtherCalls = true)
    }) { case (collocator, _) =>
      collocator.collocatedDatasets(Set(alpha1)) should be (collocatedDatasets(Set(alpha1, bravo1)))
    }
  }

  test("collocatedDatasets for a dataset should be able to return its full collocated group of datasets") {
    withMocks(defaultStoreGroups, { implicit coordinator =>
      expectCDOIAlpha1WithCollocationsComplex(andNoOtherCalls = true)
    }) { case (collocator, _) =>
      collocator.collocatedDatasets(Set(alpha1)) should be (collocatedDatasets(Set(alpha1, alpha2, bravo1, charlie1, charlie2)))
    }
  }

  def request(collocations: Seq[(DatasetInternalName, DatasetInternalName)]) = CollocationRequest(collocations)

  val requestEmpty = request(Seq.empty)

  val resultApprovedEmpty = Right(CollocationResult.canonicalEmpty)
  val resultCompleted = Right(CollocationResult.canonicalEmpty.copy(status = Completed, message = Completed.message))

  val moveJobResultEmpty = Right(SecondaryMoveJobsResult(Seq.empty))

  def secondariesFromSeq(instances: Seq[String]) = {
    val s = instances.map { instance => (instance, 0L) }.toMap
    Right(Some(SecondariesOfDatasetResult(0L, 0L, Some(0L), Some(0L), s, Set.empty[String], Map.empty)))
  }

  def secondaries(instances: String*) = secondariesFromSeq(instances)

  // tests for explainCollocation(storeGroup: String, request: CollocationRequest): Either[ErrorResult, CollocationResult]
  // and
  // tests for initiateCollocation(jobId: UUID, storeGroup: String, request: CollocationRequest): (Either[ErrorResult, CollocationResult], Seq[(Move, Boolean)])

  def shouldBeApproved(maybeResult: Either[ErrorResult, CollocationResult]): Unit = {
    val result = maybeResult.right.get

    result.id should be (None)
    result.status should be (Approved)
    result.message should be (Approved.message)
  }

  def shouldBeInProgress(jobId: UUID,
                         maybeResult: Either[ErrorResult, CollocationResult],
                         moves: Seq[(Move, Boolean)]): Unit = {
    val result = maybeResult.right.get

    result.id should be (Some(jobId))
    result.status should be (InProgress)
    result.message should be (InProgress.message)

    moves should be (result.moves.map { move => (move, true) })
  }

  def expectSecondaryMoveJobsByDataset(storeGroup: String,
                                       datasetId: DatasetInternalName,
                                       result: Either[ErrorResult, SecondaryMoveJobsResult] = moveJobResultEmpty)(implicit coordinator: Coordinator): Unit = {
    (coordinator.secondaryMoveJobs _: (String, DatasetInternalName) => Either[ErrorResult, SecondaryMoveJobsResult]).expects(storeGroup, datasetId).once.returns(result)
  }

  def expectDatasetMaxCost(storeGroup: String, datasetId: DatasetInternalName, totalSizeBytes: Long)(implicit metric: Metric): Unit = {
    (metric.datasetMaxCost _).expects(storeGroup, datasetId).once.returns(Right(Cost(1, totalSizeBytes)))
  }

  def expectDatasetMaxCostAlpha1WithCollocationsComplex(implicit metric: Metric): Unit = {
    expectDatasetMaxCost(storeGroupA, alpha1, 10)
    expectDatasetMaxCost(storeGroupA, charlie1, 10)
    expectDatasetMaxCost(storeGroupA, charlie2, 10)
    expectDatasetMaxCost(storeGroupA, bravo1, 10)
    expectDatasetMaxCost(storeGroupA, alpha2, 10)
  }

  def expectDatasetMaxCostBravo2WithCollocationsSimple(implicit metric: Metric): Unit = {
    expectDatasetMaxCost(storeGroupA, bravo2, 10)
    expectDatasetMaxCost(storeGroupA, charlie3, 10)
  }

  def testExplainAndInitiateCollocation(message: String,
                                        commonCoordinatorExpectations: Coordinator => Unit,
                                        commonMetricExpectations: Metric => Unit = { _ => },
                                        initiateCoordinatorExpectations: Coordinator => Unit = { _ => })
                                       (request: CollocationRequest,
                                        commonShould: Either[ErrorResult, CollocationResult] => Unit,
                                        explainShould: Either[ErrorResult, CollocationResult] => Unit = shouldBeApproved,
                                        initiateShould: (UUID, Either[ErrorResult, CollocationResult], Seq[(Move, Boolean)]) => Unit = shouldBeInProgress): Unit = {
    test("explainCollocation " + message) {
      withMocks(Set(storeGroupA), { coordinator =>
        commonCoordinatorExpectations(coordinator)
      }, commonMetricExpectations) { case (collocator, _) =>
        val result = collocator.explainCollocation(storeGroupA, request)

        commonShould(result)
        explainShould(result)
      }
    }

    test("initiateCollocation " + message) {
      withMocks(Set(storeGroupA), { coordinator =>
        commonCoordinatorExpectations(coordinator)
        initiateCoordinatorExpectations(coordinator)
      }, commonMetricExpectations) { case (collocator, _) =>
        val jobId = UUID.randomUUID()
        val (result, moves) = collocator.initiateCollocation(jobId, storeGroupA, request)

        commonShould(result)
        initiateShould(jobId, result, moves)
      }
    }
  }

  testExplainAndInitiateCollocation(
    "should return the canonical empty result for an empty request",
    { coordinator =>
      (coordinator.secondaryGroupConfigs _).expects().returns(Map(storeGroupA -> groupConfig(2, storesGroupA)))
    }) (

    requestEmpty,

    { commonResult => },

    { explainResult =>

      explainResult should be (resultApprovedEmpty)

    }, { (_, initiateResult, moves) =>

      initiateResult should be (resultCompleted)
      moves should be (Seq.empty)
    }
  )

  testExplainAndInitiateCollocation(
    "for two non-collocated datasets with no overlapping stores should move one dataset twice",
    { implicit coordinator =>
      (coordinator.secondaryGroupConfigs _).expects().returns(Map(storeGroupA -> groupConfig(2, storesGroupA)))

      expectSecondaryMoveJobsByDataset(storeGroupA, alpha1)
      (coordinator.secondariesOfDataset _).expects(alpha1).once.returns(secondaries(store1A, store3A))

      expectSecondaryMoveJobsByDataset(storeGroupA, bravo1)
      (coordinator.secondariesOfDataset _).expects(bravo1).once.returns(secondaries(store5A, store7A))

      expectCDOIDatasetsWithNoCollocations(Set(alpha1)) // group for alpha.1
      expectCDOIDatasetsWithNoCollocations(Set(bravo1)) // group for bravo.1

    }, { implicit metric =>
      expectDatasetMaxCost(storeGroupA, alpha1, 10)
      expectDatasetMaxCost(storeGroupA, bravo1, 10)

    }, { initiateCoordinator =>
      (initiateCoordinator.ensureSecondaryMoveJob _).expects(storeGroupA, *, *).twice.returns(Right(Right(true)))
    }) (

    request(Seq((alpha1, bravo1))),

    { commonResult =>
      val result = commonResult.right.get

      result.cost should be(Cost(2, 20, Some(10)))

      result.moves.size should be(2)
      result.moves.map(_.datasetInternalName).toSet.size should be(1)
      result.moves.map(_.storeIdTo).size should be(2)
    }
  )

  testExplainAndInitiateCollocation(
    "for two non-collocated datasets with one overlapping store should move one dataset once",
    { implicit coordinator =>
      (coordinator.secondaryGroupConfigs _).expects().returns(Map(storeGroupA -> groupConfig(2, storesGroupA)))

      expectSecondaryMoveJobsByDataset(storeGroupA, alpha1)
      (coordinator.secondariesOfDataset _).expects(alpha1).once.returns(secondaries(store1A, store3A))

      expectSecondaryMoveJobsByDataset(storeGroupA, bravo1)
      (coordinator.secondariesOfDataset _).expects(bravo1).once.returns(secondaries(store3A, store5A))

      expectCDOIDatasetsWithNoCollocations(Set(alpha1)) // group for alpha.1
      expectCDOIDatasetsWithNoCollocations(Set(bravo1)) // group for bravo.1

    }, { implicit metric =>
      expectDatasetMaxCost(storeGroupA, alpha1, 10)
      expectDatasetMaxCost(storeGroupA, bravo1, 10)


    }, { initiateCoordinator =>
      (initiateCoordinator.ensureSecondaryMoveJob _).expects(storeGroupA, *, *).once.returns(Right(Right(true)))
    }) (

    request(Seq((alpha1, bravo1))),

    { commonResult =>
      val result = commonResult.right.get

      result.cost should be (Cost(1, 10, Some(10)))

      result.moves.size should be (1)
      val move = result.moves.head
      Set(move.storeIdFrom, move.storeIdTo) should be (Set(store1A, store5A))
    }
  )

  testExplainAndInitiateCollocation(
    "for non-collocated datasets on the same stores should generate no moves",
    { implicit coordinator =>
      (coordinator.secondaryGroupConfigs _).expects().returns(Map(storeGroupA -> groupConfig(2, storesGroupA)))

      expectSecondaryMoveJobsByDataset(storeGroupA, alpha1)
      (coordinator.secondariesOfDataset _).expects(alpha1).once.returns(secondaries(store1A, store3A))

      expectSecondaryMoveJobsByDataset(storeGroupA, bravo1)
      (coordinator.secondariesOfDataset _).expects(bravo1).once.returns(secondaries(store1A, store3A))

      expectCDOIDatasetsWithNoCollocations(Set(alpha1)) // group for alpha.1
      expectCDOIDatasetsWithNoCollocations(Set(bravo1)) // group for bravo.1

    }, { implicit metric =>
      expectDatasetMaxCost(storeGroupA, alpha1, 10)
      expectDatasetMaxCost(storeGroupA, bravo1, 10)

    }) (

    request(Seq((alpha1, bravo1))),

    { commonResult => },

    { explainResult =>

      explainResult should be (resultApprovedEmpty)

    }, { (_, initateResult, moves) =>

      initateResult should be (resultCompleted)
      moves should be (Seq.empty)
    }
  )

  testExplainAndInitiateCollocation(
    "for already collocated datasets should return a result with completed status",
    { implicit coordinator =>
      (coordinator.secondaryGroupConfigs _).expects().returns(Map(storeGroupA -> groupConfig(2, storesGroupA)))

      expectSecondaryMoveJobsByDataset(storeGroupA, alpha1)
      (coordinator.secondariesOfDataset _).expects(alpha1).once.returns(secondaries(store1A, store3A))

      expectSecondaryMoveJobsByDataset(storeGroupA, bravo1)
      (coordinator.secondariesOfDataset _).expects(bravo1).once.returns(secondaries(store1A, store3A))

      expectCDOIAlpha1WithCollocationsComplex() // group for alpha.1
      expectCDOIBravo1WithCollocationsComplex() // group for bravo.1

    }, { implicit metric =>
      expectDatasetMaxCostAlpha1WithCollocationsComplex
      expectDatasetMaxCostAlpha1WithCollocationsComplex // this confuses me

    }) (

    request(Seq((alpha1, bravo1))),

    { commonResult =>

      commonResult should be(resultCompleted)

    }, { explainCompleted => },

    { (_, _, initiatedMoves) =>
      initiatedMoves should be (Seq.empty)
    }
  )

  // collocate: alpha.1 (store1A, store3A) with bravo.2 (store5A, store7A)
  //
  // moves:
  //   bravo.2: store5A, store7A -> store1A, store3A
  //
  testExplainAndInitiateCollocation(
    "for a collocated dataset and a non-collocated dataset should move the non-collocated dataset twice",
    { implicit coordinator =>
      (coordinator.secondaryGroupConfigs _).expects().returns(Map(storeGroupA -> groupConfig(2, storesGroupA)))

      expectSecondaryMoveJobsByDataset(storeGroupA, alpha1)
      (coordinator.secondariesOfDataset _).expects(alpha1).once.returns(secondaries(store1A, store3A))

      expectSecondaryMoveJobsByDataset(storeGroupA, bravo2)
      (coordinator.secondariesOfDataset _).expects(bravo2).once.returns(secondaries(store5A, store7A))

      expectCDOIAlpha1WithCollocationsComplex() // group for alpha.1
      expectCDOIDatasetsWithNoCollocations(Set(bravo2)) // group for bravo.2

    }, { implicit metric =>
      expectDatasetMaxCostAlpha1WithCollocationsComplex
      expectDatasetMaxCost(storeGroupA, bravo2, 10)

    }, { initiateCoordinator =>
      (initiateCoordinator.ensureSecondaryMoveJob _).expects(storeGroupA, bravo2, *).twice.returns(Right(Right(true)))
    }) (

    request(Seq((alpha1, bravo2))),

    { commonResult =>
      val result = commonResult.right.get

      result.cost should be (Cost(2, 20, Some(10)))

      result.moves.size should be (2)
      result.moves.map(_.datasetInternalName).toSet should be (Set(bravo2))
      result.moves.map(_.storeIdFrom).toSet should be (Set(store5A, store7A))
      result.moves.map(_.storeIdTo).toSet should be (Set(store1A, store3A))
    }
  )

  // collocate: alpha.1 (store1A, store3A) with bravo.2 (store5A, store7A)
  //
  // moves:
  //   bravo.2:   store5A, store7A -> store1A, store3A
  //   charlie.3: store5A, store7A -> store1A, store3A
  //
  testExplainAndInitiateCollocation(
    "for two disjointly collocated datasets should move the dataset with the smaller collocated group",
    { implicit coordinator =>
      (coordinator.secondaryGroupConfigs _).expects().returns(Map(storeGroupA -> groupConfig(2, storesGroupA)))

      expectSecondaryMoveJobsByDataset(storeGroupA, alpha1)
      (coordinator.secondariesOfDataset _).expects(alpha1).once.returns(secondaries(store1A, store3A))

      expectSecondaryMoveJobsByDataset(storeGroupA, bravo2)
      (coordinator.secondariesOfDataset _).expects(bravo2).once.returns(secondaries(store5A, store7A))

      expectCDOIAlpha1WithCollocationsComplex() // group for alpha.1
      expectCDOIBravo2WithCollocationsSimple() // group for bravo.2

    }, { implicit metric =>
      expectDatasetMaxCostAlpha1WithCollocationsComplex
      expectDatasetMaxCostBravo2WithCollocationsSimple

    }, { initiateCoordinator =>
      (initiateCoordinator.ensureSecondaryMoveJob _).expects(storeGroupA, bravo2, *).twice.returns(Right(Right(true)))
      (initiateCoordinator.ensureSecondaryMoveJob _).expects(storeGroupA, charlie3, *).twice.returns(Right(Right(true)))
    }) (

    request(Seq((alpha1, bravo2))),

    { commonResult =>
      val result = commonResult.right.get

      result.cost should be (Cost(4, 40, Some(10)))

      result.moves.size should be (4)
      result.moves.map(_.datasetInternalName).toSet should be (Set(bravo2, charlie3))
      result.moves.map(_.storeIdFrom).toSet should be (Set(store5A, store7A))
      result.moves.map(_.storeIdTo).toSet should be (Set(store1A, store3A))
    }
  )

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
    withMocks(defaultStoreGroups, expectNoMockCoordinatorCalls, expectNoMockMetricCalls, { lock =>
      (lock.acquire _).expects(10000L).once.returns(true)

    }) { (collocator, _) =>
      collocator.beginCollocation()
    }
  }

  test("beginCollocation should throw a CollocationLockTimeout exception if it cannot acquire the collocation lock") {
    withMocks(defaultStoreGroups, expectNoMockCoordinatorCalls, expectNoMockMetricCalls, { lock =>
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
    withMocks(defaultStoreGroups, expectNoMockCoordinatorCalls, expectNoMockMetricCalls, { lock =>
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

  val bravo2Cost = Cost(1, 10L)

  val moveBravo2To1A = Move(bravo2, store7A, store1A, bravo2Cost)
  val moveBravo2To3A = Move(bravo2, store5A, store3A, bravo2Cost)

  val moveCharlie3To1A = Move(charlie3, store7A, store1A, bravo2Cost)
  val moveCharlie3To3A = Move(charlie3, store5A, store3A, bravo2Cost)

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
