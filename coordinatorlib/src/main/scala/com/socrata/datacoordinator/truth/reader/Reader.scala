package com.socrata.datacoordinator
package truth.reader

import java.io.Closeable

import com.socrata.datacoordinator.util.CloseableIterator

trait Reader[CV] extends Closeable {
  def lookupBySystemId(columns: Iterable[String], ids: Iterator[Long]): CloseableIterator[Seq[(Long, Option[Row[CV]])]]
  def lookupByUserId(columns: Iterable[String], ids: Iterator[CV]): CloseableIterator[Seq[(CV, Option[Row[CV]])]]
}
