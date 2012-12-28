package com.socrata.datacoordinator
package truth.reader

import java.io.Closeable

import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.datacoordinator.id.{RowId, ColumnId}

trait Reader[CV] extends Closeable {
  def lookupBySystemId(columns: Iterable[ColumnId], ids: Iterator[RowId]): CloseableIterator[Seq[(RowId, Option[Row[CV]])]]
  def lookupByUserId(columns: Iterable[ColumnId], ids: Iterator[CV]): CloseableIterator[Seq[(CV, Option[Row[CV]])]]
}
