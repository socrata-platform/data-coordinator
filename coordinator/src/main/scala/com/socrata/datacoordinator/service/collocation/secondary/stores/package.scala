package com.socrata.datacoordinator.service.collocation.secondary

import scala.annotation.tailrec

package object stores {
  def randomStore(stores: Set[String]): Option[String] = scala.util.Random.shuffle(stores.toSeq).headOption

  def randomStores(stores: Set[String], count: Int): Option[Set[String]] = {
    @tailrec
    def select(selected: Set[String], available: Set[String], left: Int): Set[String] = {
      if (left > 0) {
        val store = randomStore(available).get
        select(selected + store, available - store, left - 1)
      } else {
        selected
      }
    }

    if (stores.size < count) None
    else Some(select(selected = Set.empty[String], available = stores, left = count))
  }
}
