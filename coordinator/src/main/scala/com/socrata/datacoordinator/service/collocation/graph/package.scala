package com.socrata.datacoordinator.service.collocation

import scala.annotation.tailrec

package object graph {

  /**
    * Returns the Nodes of the connected component
    * starting at the root Node for the undirected
    * graph specified by its edges.
    */
  def componentNodes[T](rootNode: T, edges: Set[Set[T]]): Set[T] = {
    @tailrec
    def nodes(currentNodes: Set[T], seen: Set[T]): Set[T] = {
      val nodesToExplore = edges.filter(_.intersect(currentNodes).nonEmpty).flatten -- seen
      if (nodesToExplore.nonEmpty) nodes(nodesToExplore, seen ++ nodesToExplore)
      else seen
    }

    nodes(currentNodes = Set(rootNode), seen = Set(rootNode))
  }

  /**
    * Returns all Nodes grouped by their connected
    * component for the undirected graph specified
    * by its edges.
    */
  def nodesByComponent[T](edges: Set[Set[T]]): Set[Set[T]] = {
    @tailrec
    def byComponent(nodesToExplore: Set[T], seen: Set[Set[T]]): Set[Set[T]] = {
      if (nodesToExplore.nonEmpty) {
        val newComponentNodes = componentNodes(nodesToExplore.head, edges)
        byComponent(nodesToExplore -- newComponentNodes, seen + newComponentNodes)
      } else {
        seen
      }
    }

    byComponent(nodesToExplore = edges.flatten, seen = Set.empty)
  }
}
