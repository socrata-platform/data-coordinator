package com.socrata.datacoordinator.service.collocation.secondary.stores

import com.socrata.datacoordinator.secondary.config.SecondaryGroupConfig
import com.socrata.datacoordinator.service.collocation.Cost

import scala.annotation.tailrec

case class NotEnoughInstancesInSecondaryGroup(name: String, count: Int)
  extends Exception(s"Unable to find $count available instances in secondary store group $name.")

class SecondaryStoreSelector(groupName: String,
                             allStores: Set[String],
                             unavailableStores: Set[String],
                             replicationFactor: Int) {

  def maxCostKey[T](costMap: Map[T, Cost]): Option[T] =
    if (costMap.nonEmpty) Some(costMap.maxBy(_._2)._1) // max by Cost and return key
    else None

  def invalidDestinationStore[T](store: String, storeMap: Map[T, Set[String]]): Boolean =
    unavailableStores(store) && !storeMap.forall(_._2.contains(store))

  def destinationStores[T](storeMap: Map[T, Set[String]], costMap: Map[T, Cost]): Set[String] = {
    @tailrec
    def store(toExplore: Map[T, Cost], seen: Set[String]): (String, Set[String]) = {
      if (toExplore.nonEmpty) {
        val currentKey = maxCostKey(toExplore).get
        val currentStores = storeMap(currentKey)

        val possibleStores = currentStores.filterNot { s => seen(s) || invalidDestinationStore(s, storeMap) }
        randomStore(possibleStores) match {
          case Some(selected) =>
            val nowSeen = seen ++ (currentStores -- possibleStores) + selected
            (selected, nowSeen)
          case None =>
            val nowSeen = seen ++ currentStores
            store(toExplore.filterNot(_._1 == currentKey), nowSeen)
        }
      } else {
        randomStore(allStores -- (unavailableStores ++ seen)) match {
          case Some(selected) => (selected, seen + selected)
          case None =>  throw NotEnoughInstancesInSecondaryGroup(groupName, replicationFactor)
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
  def apply(groupName: String, groupConfig: SecondaryGroupConfig): SecondaryStoreSelector =
    new SecondaryStoreSelector(
      groupName = groupName,
      allStores = groupConfig.instances,
      unavailableStores = groupConfig.instancesNotAcceptingNewDatasets.getOrElse(Set.empty),
      replicationFactor = groupConfig.numReplicas
    )
}
