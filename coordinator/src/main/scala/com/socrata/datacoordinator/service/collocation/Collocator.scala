package com.socrata.datacoordinator.service.collocation

import java.util.UUID

import com.rojoma.json.v3.util._
import com.socrata.datacoordinator.common.collocation.{CollocationLock, CollocationLockError, CollocationLockTimeout}
import com.socrata.datacoordinator.id.DatasetInternalName
import com.socrata.datacoordinator.resources.collocation.{CollocatedDatasetsResult, DatasetNotInStore, SecondaryMoveJobRequest, StoreNotAcceptingDatasets}
import com.socrata.datacoordinator.service.collocation.secondary.stores.SecondaryStoreSelector

import scala.annotation.tailrec

@JsonKeyStrategy(Strategy.Underscore)
case class CollocationRequest(collocations: Seq[(DatasetInternalName, DatasetInternalName)])

object CollocationRequest {
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
    cost = Cost(0),
    moves = Seq.empty
  )

  def canonicalEmpty: CollocationResult = CollocationResult(
    id = None,
    status = Completed,
    cost = Cost(0),
    moves = Seq.empty
  )
}

trait Collocator {
  def collocatedDatasets(datasets: Set[DatasetInternalName]): Either[RequestError, CollocatedDatasetsResult]
  def defaultStoreGroups: Set[String]
  def explainCollocation(storeGroup: String, request: CollocationRequest): Either[ErrorResult, CollocationResult]
  def initiateCollocation(jobId: UUID, storeGroup: String, request: CollocationRequest): (Either[ErrorResult, CollocationResult], Seq[(Move, Boolean)])
  def saveCollocation(request: CollocationRequest): Unit
  def beginCollocation(): Unit
  def commitCollocation(): Unit
  def rollbackCollocation(jobId: UUID, moves: Seq[(Move, Boolean)]): Option[ErrorResult]
}

trait CollocatorProvider {
  val collocator: Collocator
}

class CoordinatedCollocator(collocationGroup: Set[String],
                            override val defaultStoreGroups: Set[String],
                            coordinator: Coordinator,
                            addCollocations: Seq[(DatasetInternalName, DatasetInternalName)] => Unit,
                            lock: CollocationLock,
                            lockTimeoutMillis: Long) extends Collocator {

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
        dataset.toString, replicationFactor.toString, storesInGroup.size.toString)
      throw new Exception("Dataset replicated to stores in an unexpected state!")
    }

    storesInGroup
  }

  protected def movesFor(group: Set[DatasetInternalName], storesFrom: Set[String], storesTo: Set[String]): Seq[Move] = {
    assert(storesFrom.size == storesTo.size)

    scala.util.Random.shuffle((storesFrom -- storesTo).toSeq).
      zip((storesTo -- storesFrom).toSeq).
      flatMap { stores =>
        val (storeFrom, storeTo) = stores
        group.map(Move(_, storeFrom, storeTo))
      }
  }

  override def explainCollocation(storeGroup: String,
                                  request: CollocationRequest): Either[ErrorResult, CollocationResult] = {
    log.info("Explaining collocation on secondary store group {}", storeGroup)
    try {
      coordinator.secondaryGroups.get(storeGroup) match {
        case Some(groupConfig) =>
          val instances = groupConfig.instances
          val replicationFactor = groupConfig.numReplicas

          request match {
            case CollocationRequest(collocations) if collocations.isEmpty => Right(CollocationResult.canonicalEmpty)
            case CollocationRequest(collocations) =>
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
                // TODO: implement useful cost (i.e related to size of replicated dataset), work tracked in EN-21686
                (dataset, Cost(moves = 1))
              }.toMap

              // represents graph of collocated groups to be collocated
              val collocatedGroupsEdges = collocationEdges.map(_.map(datasetGroupMap)).filter(_.size == 2)

              val groupsToBeCollocated = graph.nodesByComponent(collocatedGroupsEdges)
              val (totalCost, totalMoves) = groupsToBeCollocated.flatMap { groups =>
                // from the filter condition above groups will have at least size 2
                assert(groups.size >= 2)

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

                val destinationStores = SecondaryStoreSelector(storeGroup, groupConfig)
                  .destinationStores(groupStoreMap, groupCostMap)

                // cost and moves to move each other group to the most expensive group
                indexedGroups.map { case (group, index) =>
                  val moves = movesFor(group, storesFrom = groupStoreMap(index), storesTo = destinationStores)
                  val cost = moves.foldLeft(Cost.Zero) { case (groupCost, move) =>
                    groupCost + datasetCostMap(move.datasetInternalName)
                  }

                  (cost, moves)
                }
              }.fold((Cost.Zero, Seq.empty[Move])) { case ((xCost, xMoves), (yCost, yMoves)) =>
                (xCost + yCost, xMoves ++ yMoves)
              }

              if (totalCost == Cost.Zero) return Right(CollocationResult.canonicalEmpty)

              Right(CollocationResult(
                id = None,
                status = Approved,
                message = Approved.message,
                cost = totalCost,
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

  override def initiateCollocation(jobId: UUID,
                                  storeGroup: String,
                                  request: CollocationRequest): (Either[ErrorResult, CollocationResult], Seq[(Move, Boolean)]) = {
    log.info("Executing collocation on secondary store group {}", storeGroup)
    explainCollocation(storeGroup, request) match {
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
          cost = cost,
          moves = moves
        )), successfulMoves)
      case other => (other, Seq.empty)
    }
  }

  override def saveCollocation(request: CollocationRequest): Unit = {
    // save the collocation relationships to the database after ensuring all required moves jobs exists
    // this way we only need to roll back jobs for our job id back if something goes wrong
    log.info("Adding collocation relationships to the manifest: {}", request.collocations)
    addCollocations(request.collocations)
  }

  override def beginCollocation(): Unit = {
    try {
      log.info("Attempting to acquire collocation lock to begin collocation.")
      if (lock.acquire(lockTimeoutMillis)) {
        log.info("Acquired collocation lock to begin collocation.")
      } else {
        log.warn("Timeout while waiting to acquire collocation lock to begin collocation.")
        throw CollocationLockTimeout(lockTimeoutMillis)
      }
    } catch {
      case error: CollocationLockError =>
        log.error("Unexpected error while acquiring collocation lock for collocation.", error)
        throw error
    }
  }

  override def commitCollocation(): Unit = {
    try {
      log.info("Releasing collocation lock to commit collocation.")
      lock.release()
    } catch {
      case error: CollocationLockError =>
        log.error("Unexpected error while releasing collocation lock for collocation.", error)
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
