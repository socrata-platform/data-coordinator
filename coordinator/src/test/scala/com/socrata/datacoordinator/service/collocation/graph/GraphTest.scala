package com.socrata.datacoordinator.service.collocation.graph

import org.scalatest.{FunSuite, Matchers}

class GraphTest extends FunSuite with Matchers {

  val edgesEmpty = Set.empty[Set[Int]]

  test("For a graph with no edges the connected component of a node is just the node") {
    componentNodes(rootNode = 0, edgesEmpty) should be (Set(0))
  }

  test("For a graph with no edges the connected components is awkwardly the empty set") {
    nodesByComponent(edgesEmpty) should be (edgesEmpty) // this is kind of awkward... but doesn't matter to us
  }

  val nodesConnected = Set(0, 1, 2, 3)
  val edgesConnected = Set(
    Set(0, 1),
    Set(1, 2),
    Set(2, 0),
    Set(2, 3)
  )

  test("For a connected graph, the connected component of a node is just the whole graph") {
    componentNodes(rootNode = 0, edgesConnected) should be (nodesConnected)
    componentNodes(rootNode = 1, edgesConnected) should be (nodesConnected)
    componentNodes(rootNode = 2, edgesConnected) should be (nodesConnected)
    componentNodes(rootNode = 3, edgesConnected) should be (nodesConnected)
  }

  test("For a connected graph there is only one connected component -- the graph itself") {
    nodesByComponent(edgesConnected) should be (Set(nodesConnected))
  }

  val edgesDisconnected = Set(
    Set(0, 1),
    Set(1, 2),
    Set(3, 4),
    Set(5, 6),
    Set(5, 7),
    Set(7, 8),
    Set(8, 5)
  )

  test("For a disconnected graph, the connected component of a node is the disconnected subgraph containing the node") {
    componentNodes(rootNode = 0, edgesDisconnected) should be (Set(0, 1, 2))
    componentNodes(rootNode = 3, edgesDisconnected) should be (Set(3, 4))
    componentNodes(rootNode = 7, edgesDisconnected) should be (Set(5, 6, 7, 8))
  }

  test("For a disconnected graph, the connected components are the disconnected subgraphs") {
    nodesByComponent(edgesDisconnected) should be (Set(Set(0, 1, 2), Set(3, 4), Set(5, 6, 7, 8)))
  }
}
