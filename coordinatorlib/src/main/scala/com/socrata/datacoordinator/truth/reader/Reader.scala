package com.socrata.datacoordinator
package truth.reader

import com.socrata.datacoordinator.util.CloseableIterator

trait Reader[CV] {
  def lookupBySystemId(columns: Iterable[String], ids: Iterable[Long]): CloseableIterator[Seq[Row[CV]]]
  def lookupByUserId(columns: Iterable[String], ids: Iterable[CV]): CloseableIterator[Seq[Row[CV]]]
}
