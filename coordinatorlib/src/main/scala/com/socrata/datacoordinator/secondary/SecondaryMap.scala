package com.socrata.datacoordinator
package secondary

import com.socrata.datacoordinator.id.StoreId

trait SecondaryMap {
  def create(storeId: String, wantsUnpublished: Boolean): StoreId
  def lookup(storeId: String): Option[(StoreId, Boolean)] // ick but I'm in a rush
  def lookup(storeId: StoreId): Option[(String, Boolean)] // ick but I'm in a rush
}
