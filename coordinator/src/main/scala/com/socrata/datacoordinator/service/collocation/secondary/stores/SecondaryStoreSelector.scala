package com.socrata.datacoordinator.service.collocation.secondary.stores

import org.slf4j.LoggerFactory

import com.socrata.datacoordinator.secondary.SecondaryMetric
import com.socrata.datacoordinator.secondary.config.SecondaryGroupConfig
import com.socrata.datacoordinator.service.collocation.Cost

import scala.annotation.tailrec

case class NotEnoughInstancesInSecondaryGroup(name: String, count: Int)
  extends Exception(s"Unable to find $count available instances in secondary store group $name.")

class SecondaryStoreSelector(groupName: String,
                             storesFreeSpaceMap: Map[String, Long],
                             replicationFactor: Int)(implicit costOrdering: Ordering[Cost]) {
  import SecondaryStoreSelector.log

  def maxCostKey[T](costMap: Map[T, Cost]): Option[T] =
    if (costMap.nonEmpty) Some(costMap.maxBy(_._2)._1) // max by Cost and return key
    else None

  private def unavailableStores(cost: Cost): Set[String] =
    storesFreeSpaceMap.filter(cost.totalSizeBytes > _._2).keySet

  def invalidDestinationStore[T](store: String, storeMap: Map[T, Set[String]], costMap: Map[T, Cost]): Boolean = {
    val cost = costMap.filterKeys(!storeMap(_).contains(store)).values.fold(Cost.Zero)(_ + _)
    unavailableStores(cost)(store) && !storeMap.forall(_._2.contains(store))
  }

  def destinationStores[T](storeMap: Map[T, Set[String]], costMap: Map[T, Cost]): Set[String] = {
    @tailrec
    def store(toExplore: Map[T, Cost], seen: Set[String]): (String, Set[String]) = {
      if (toExplore.nonEmpty) {
        val currentKey = maxCostKey(toExplore).get
        val currentStores = storeMap(currentKey)

        val possibleStores = currentStores.filterNot { s => seen(s) || invalidDestinationStore(s, storeMap, costMap) }
        randomStore(possibleStores) match {
          case Some(selected) =>
            val nowSeen = seen ++ (currentStores -- possibleStores) + selected
            (selected, nowSeen)
          case None =>
            val nowSeen = seen ++ currentStores
            store(toExplore.filterNot(_._1 == currentKey), nowSeen)
        }
      } else {
        val allStores = storesFreeSpaceMap.keySet
        val totalCost = costMap.values.fold(Cost.Zero)(_ + _)
        randomStore(allStores -- (unavailableStores(totalCost) ++ seen)) match {
          case Some(selected) => (selected, seen + selected)
          case None =>
            log.info("Total cost: {}, all unchosen stores: {}", totalCost:Any, storesFreeSpaceMap -- seen)
            throw NotEnoughInstancesInSecondaryGroup(groupName, replicationFactor)
        }
      }
    }

    assert(storeMap.keySet == costMap.keySet)

    var seen = Set.empty[String]
    val destinations = (1 to replicationFactor).map { _ =>
      val (destination, nowSeen) = store(costMap, seen)
      seen = nowSeen

      destination
    }.toSet

    assert(destinations.size == replicationFactor)

    destinations
  }
}

object SecondaryStoreSelector {
  val log = LoggerFactory.getLogger(classOf[SecondaryStoreSelector])

  def apply(groupName: String,
            groupConfig: SecondaryGroupConfig,
            storeMetrics: Map[String, SecondaryMetric])(implicit costOrdering: Ordering[Cost]): SecondaryStoreSelector = {
    val storesFreeSpaceMap = groupConfig.instances.map { case (storeId, storeConfig) =>
      val freeSpaceBytes = if (storeConfig.acceptingNewDatasets)
        storeConfig.storeCapacityMB * 1024L * 1024L - storeMetrics(storeId).totalSizeBytes
      else 0L

      (storeId, freeSpaceBytes)
    }
    new SecondaryStoreSelector(groupName, storesFreeSpaceMap, groupConfig.numReplicas)
  }
}
