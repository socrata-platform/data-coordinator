package com.socrata.datacoordinator.service.collocation

import java.util.UUID

import com.rojoma.json.v3.util._
import com.socrata.datacoordinator.common.collocation.{CollocationLock, CollocationLockError, CollocationLockTimeout}
import com.socrata.datacoordinator.id.DatasetInternalName
import com.socrata.datacoordinator.resources.collocation.{CollocatedDatasetsResult, DatasetNotInStore, SecondaryMoveJobRequest, StoreNotAcceptingDatasets}
import com.socrata.datacoordinator.service.collocation.secondary.stores.SecondaryStoreSelector

import scala.annotation.tailrec

@JsonKeyStrategy(Strategy.Underscore)
case class CollocationRequest(collocations: Seq[(DatasetInternalName, DatasetInternalName)],
                              limits: CollocationRequest.CostLimits)

object CollocationRequest {
  type CostLimits = Cost

  implicit val costLimitsDecode = AutomaticJsonDecodeBuilder[CostLimits]
  implicit val decode = AutomaticJsonDecodeBuilder[CollocationRequest]
}

case class CollocationResult(id: Option[UUID], status: Status, message: String, cost: Cost, moves: Seq[Move]) {

  def +(that: CollocationResult): CollocationResult = {
    CollocationResult(
      id = this.id,
      status = Status.combine(this.status, that.status),
      cost = this.cost + that.cost,
      moves = this.moves ++ that.moves
    )
  }
}

object CollocationResult {
  implicit val encode = AutomaticJsonEncodeBuilder[CollocationResult]

  def apply(id: Option[UUID], status: Status, cost: Cost, moves: Seq[Move]): CollocationResult = {
    CollocationResult(id, status, status.message, cost, moves)
  }

  def apply(id: UUID): CollocationResult = CollocationResult(
    id = Some(id),
    status = Completed,
    cost = Cost.Zero,
    moves = Seq.empty
  )

  def canonicalEmpty: CollocationResult = CollocationResult(
    id = None,
    status = Completed,
    cost = Cost.Zero,
    moves = Seq.empty
  )
}

trait Collocator {
  def collocatedDatasets(datasets: Set[DatasetInternalName]): Either[RequestError, CollocatedDatasetsResult]
  def dropDataset(dataset: DatasetInternalName): Option[ErrorResult]
  def explainCollocation(jobId: UUID, storeGroup: String, request: CollocationRequest): Either[ErrorResult, CollocationResult]
  def executeCollocation(jobId: UUID, storeGroup: String, request: CollocationRequest): (Either[ErrorResult, CollocationResult], Seq[(Move, Boolean)])
  def commitCollocation(jobId: UUID, request: CollocationRequest): Unit
  def lockCollocation(): Unit
  def unlockCollocation(): Unit
  def rollbackCollocation(jobId: UUID, moves: Seq[(Move, Boolean)]): Option[ErrorResult]
}

trait CollocatorProvider {
  val collocator: Collocator
}

class CoordinatedCollocator(collocationGroup: Set[String],
                            coordinator: Coordinator,
                            metric: Metric,
                            addCollocations: (UUID, Seq[(DatasetInternalName, DatasetInternalName)]) => Unit,
                            lock: CollocationLock,
                            lockTimeoutMillis: Long)(implicit costOrdering: Ordering[Cost]) extends Collocator {

  private val log = org.slf4j.LoggerFactory.getLogger(classOf[Collocator])

  override def collocatedDatasets(datasets: Set[DatasetInternalName]): Either[RequestError, CollocatedDatasetsResult] = {
    try {
      @tailrec
      def search(current: Set[DatasetInternalName], seen: Set[DatasetInternalName]): Set[DatasetInternalName] = {
        val toExplore = collocationGroup.flatMap { instance =>
          coordinator.collocatedDatasetsOnInstance(instance, current).fold(throw _, _.datasets)
        } -- seen

        if (toExplore.nonEmpty) search(toExplore, seen ++ toExplore)
        else seen
      }

      Right(CollocatedDatasetsResult(search(current = datasets, seen = datasets)))
    } catch {
      case error: RequestError => Left(error)
    }
  }

  private def secondariesOfDataset(dataset: DatasetInternalName): Set[String] =
    coordinator.secondariesOfDataset(dataset).fold(throw _, _.getOrElse(throw DatasetNotFound(dataset)).secondaries.keySet)

  protected def stores(storeGroup: String, dataset: DatasetInternalName, instances: Set[String], replicationFactor: Int): Set[String] = {
    // we want to get jobs _before_ current stores
    val jobs = coordinator.secondaryMoveJobs(storeGroup, dataset).fold(throw _, identity)
    val currentStores = coordinator.secondariesOfDataset(dataset).fold(throw _, _.getOrElse(throw DatasetNotFound(dataset)).secondaries.keySet)

    // apply moves to the current stores in the order they will execute in
    val futureStores = jobs.moves.sorted.foldLeft(currentStores) { (stores, move) =>
      stores - move.fromStoreId + move.toStoreId
    }

    val storesInGroup = instances.intersect(futureStores)
    if (replicationFactor != storesInGroup.size) {
      log.error("Dataset {}'s current replication factor {} is not the expected replication factor {} for the group {}",
        dataset.toString, storesInGroup.size.toString, replicationFactor.toString, storeGroup)
      throw new Exception("Dataset replicated to stores in an unexpected state!")
    }

    storesInGroup
  }

  override def dropDataset(dataset: DatasetInternalName): Option[ErrorResult] = {
    log.info("Dropping dataset {} from collocation manifests", dataset)
    collocationGroup.flatMap { instance =>
      coordinator.dropCollocationsOnInstance(instance, dataset)
    }.headOption
  }

  protected def movesFor(group: Set[DatasetInternalName],
                         storesFrom: Set[String],
                         storesTo: Set[String],
                         costMap: Map[DatasetInternalName, Cost]): Seq[Move] = {
    if (storesFrom.size != storesTo.size) {
      log.error("storesFrom.size != storesTo.size, something is wrong internally!")
      throw new IllegalArgumentException("storesFrom and storesTo should be the same size!")
    }

    for {
      (storeFrom, storeTo) <- scala.util.Random.shuffle((storesFrom -- storesTo).toSeq).zip((storesTo -- storesFrom).toSeq)
      dataset <- group
    } yield {
      Move(dataset, storeFrom, storeTo, costMap(dataset))
    }
  }

  override def explainCollocation(jobId: UUID,
                                  storeGroup: String,
                                  request: CollocationRequest): Either[ErrorResult, CollocationResult] = {
    log.info("Explaining collocation on secondary store group {}", storeGroup)
    try {
      coordinator.secondaryGroupConfigs.get(storeGroup) match {
        case Some(groupConfig) =>
          val instances = groupConfig.instances.keySet
          val replicationFactor = groupConfig.numReplicas

          request match {
            case CollocationRequest(collocations, _) if collocations.isEmpty => Right(CollocationResult.canonicalEmpty)
            case CollocationRequest(collocations, costLimits) =>
              val collocationEdges = collocations.map { collocation =>
                Set(collocation._1, collocation._2)
              }.toSet

              val inputDatasets = collocationEdges.flatten

              // we will 404 if we get a 404 projected stores for one of the input datasets
              val datasetStoresMap = inputDatasets.map { dataset =>
                if (collocationGroup(dataset.instance)) {
                  (dataset, stores(storeGroup, dataset, instances, replicationFactor))
                } else {
                  log.warn("Unable to find dataset {} since it has an unrecognized instance name!", dataset)
                  return Left(DatasetNotFound(dataset))
                }
              }.toMap

              val datasetGroupMap = inputDatasets.map { dataset =>
                (dataset, collocatedDatasets(Set(dataset)).fold(throw _, _.datasets))
              }.toMap

              // cost of _all_ datasets in question, not just the input datasets
              val datasetCostMap = datasetGroupMap.flatMap(_._2).map { dataset =>
                (dataset, metric.datasetMaxCost(storeGroup, dataset).fold(throw _, identity))
              }.toMap

              val storeMetricsMap = instances.map { instance =>
                (instance, metric.storeMetrics(instance).fold(throw _, identity))
              }.toMap

              def selectMoves(groups: Set[Set[DatasetInternalName]]): Set[Move] = {
                val indexedGroups = groups.zipWithIndex

                val groupCostMap = indexedGroups.map { case (group, index) =>
                  // cost of the group is the cost of moving each dataset (once)
                  val cost = group.map(datasetCostMap(_)).fold(Cost.Zero)(_ + _)

                  (index, cost)
                }.toMap

                val groupStoreMap = indexedGroups.map { case (group, index) =>
                  // a group should have at least on dataset in it
                  assert(group.nonEmpty)
                  // the projected stores for a group, should be the stores for one of its members
                  val representingDataset = group.intersect(inputDatasets).head

                  (index, datasetStoresMap(representingDataset))
                }.toMap

                val destinationStores = SecondaryStoreSelector(storeGroup, groupConfig, storeMetricsMap)
                  .destinationStores(groupStoreMap, groupCostMap)

                // cost and moves to move each other group to the most expensive group
                indexedGroups.flatMap { case (group, index) =>
                  movesFor(
                    group,
                    storesFrom = groupStoreMap(index),
                    storesTo = destinationStores,
                    datasetCostMap
                  )
                }
              }

              // represents graph of collocated groups to be collocated
              val collocatedGroupsEdges = collocationEdges.map(_.map(datasetGroupMap)).filter(_.size == 2)

              val groupsToBeCollocated = graph.nodesByComponent(collocatedGroupsEdges)
              val totalMoves = groupsToBeCollocated.flatMap { groups =>
                // from the filter condition above groups will have at least size 2
                assert(groups.size >= 2)
                selectMoves(groups)
              }.toSeq

              val totalCost = Move.totalCost(totalMoves)
              if (totalCost == Cost.Zero) return Right(CollocationResult.canonicalEmpty)

              val totalStatus =
                if (totalCost.moves > costLimits.moves)
                  Rejected(s"the number of moves exceed the limit ${costLimits.moves}")
                else if (totalCost.totalSizeBytes > costLimits.totalSizeBytes)
                  Rejected(s"the total size in bytes exceeds the limit ${costLimits.totalSizeBytes}")
                else if (totalCost.getMoveSizeMaxBytes > costLimits.getMoveSizeMaxBytes)
                  Rejected(s"the max move size in bytes exceeds the limit ${costLimits.getMoveSizeMaxBytes}")
                else {
                  totalMoves.groupBy(_.storeIdTo).flatMap { case (storeId, moves) =>
                    val bytesToMove = Move.totalCost(moves).totalSizeBytes
                    val storeTotalBytes = storeMetricsMap(storeId).totalSizeBytes
                    val storeCapacityBytes = groupConfig.instances(storeId).storeCapacityMB * 1024 * 1024

                    if (storeTotalBytes + bytesToMove > storeCapacityBytes)
                      Some(Rejected(s"the moves exceed the capacity of store $storeId"))
                    else None
                  }.headOption.getOrElse(Approved)
                }

              totalStatus match {
                case Rejected(reason) => log.info("Rejecting collocation request because {}", reason)
                case Approved => log.info("Approving collocation request for collocations: {}", request.collocations)
                case _ => throw new Exception("Unexpected collocation status in explain")
              }

              Right(CollocationResult(
                id = None,
                status = totalStatus,
                message = totalStatus.message,
                cost = totalCost, // here we return the full cost vector, we can obscure this further up the stack
                moves = totalMoves
              ))
          }
        case None => Left(StoreGroupNotFound(storeGroup))
      }
    } catch {
      case e: DatasetNotFound =>
        log.warn("No such dataset {}", e.internalName)
        Left(e)
      case e: RequestError =>
        log.error("Unexpected error making request during explain of collocation: {}", e)
        Left(e)
    }
  }

  override def executeCollocation(jobId: UUID,
                                  storeGroup: String,
                                  request: CollocationRequest): (Either[ErrorResult, CollocationResult], Seq[(Move, Boolean)]) = {
    log.info("Executing collocation on secondary store group {}", storeGroup)
    explainCollocation(jobId, storeGroup, request) match {
      case Right(CollocationResult(_, Approved, _, cost, moves)) =>

        log.info("Ensuring required move jobs exists: {}", moves)
        val moveResults = moves.map { move =>
          val request = SecondaryMoveJobRequest(jobId, move.storeIdFrom, move.storeIdTo)
          val result = try {
            coordinator.ensureSecondaryMoveJob(storeGroup, move.datasetInternalName, request) match {
              case Right(Right(datasetNewToStore)) => // successfully added job
                Right(datasetNewToStore)
              case Right(Left(StoreNotAcceptingDatasets)) =>
                log.error(s"Attempted to move dataset ${move.datasetInternalName} to store {} not accepting datasets!",
                  move.storeIdTo)
                Left(UnexpectedError("Attempted to move dataset to store not accepting new datasets during collocation!"))
              case Right(Left(DatasetNotInStore)) =>
                log.error(s"Attempted to move dataset ${move.datasetInternalName} from store {} that it is not in!",
                  move.storeIdFrom)
                Left(UnexpectedError("Attempted to move dataset from store that it is not in during collocation!"))
              case Left(error) => Left(error)
            }
          } catch {
            case error: Exception =>
              log.error("Unexpected exception while ensuring secondary move jobs", error)
              Left(UnexpectedError(error.getMessage))
          }

          (result, move)
        }

        val successfulMoves = moveResults.filter(_._1.isRight).map { case (result, move) => (move, result.right.get) }

        moveResults.find(_._1.isLeft) match {
          case Some(error) => return (Left(error._1.left.get), successfulMoves)
          case None =>
        }

        (Right(CollocationResult(
          id = Some(jobId),
          status = InProgress,
          message = InProgress.message,
          cost = cost, // here we return the full cost vector, we can obscure this further up the stack
          moves = moves
        )), successfulMoves)
      case other => (other, Seq.empty)
    }
  }

  override def commitCollocation(jobId: UUID, request: CollocationRequest): Unit = {
    // save the collocation relationships to the database after ensuring all required moves jobs exists
    // this way we only need to roll back jobs for our job id back if something goes wrong
    log.info("Adding collocation relationships to the manifest: {}", request.collocations)
    addCollocations(jobId, request.collocations)
  }

  override def lockCollocation(): Unit = {
    try {
      log.info("Attempting to acquire collocation lock")
      if (lock.acquire(lockTimeoutMillis)) {
        log.info("Acquired collocation lock")
      } else {
        log.warn("Timeout while waiting to acquire collocation lock")
        throw CollocationLockTimeout(lockTimeoutMillis)
      }
    } catch {
      case error: CollocationLockError =>
        log.error("Unexpected error while acquiring collocation lock ", error)
        throw error
    }
  }

  override def unlockCollocation(): Unit = {
    try {
      log.info("Releasing collocation lock")
      lock.release()
    } catch {
      case error: CollocationLockError =>
        log.error("Unexpected error while releasing collocation lock", error)
        throw error
    }
  }

  override def rollbackCollocation(jobId: UUID,
                                   moves: Seq[(Move, Boolean)]): Option[ErrorResult] = {
    val errors = moves.flatMap { case (move, dropFromStore) =>
      coordinator.rollbackSecondaryMoveJob(move.datasetInternalName.instance, jobId, move, dropFromStore)
    }

    errors.foreach { error =>
      log.error("Encountered error during rollback of collocation!!!", error)
    }

    errors.headOption
  }
}
