package com.socrata.datacoordinator
package truth.loader

import com.rojoma.simplearm.v2._

trait DatasetExtractor[CV] {
  def allRows(limit: Option[Long], offset: Option[Long], sorted: Boolean, rowId: Option[CV], rs: ResourceScope): Iterator[Row[CV]]

  final def allRows(limit: Option[Long], offset: Option[Long], sorted: Boolean, rowId: Option[CV]): Managed[Iterator[Row[CV]]] =
    new Managed[Iterator[Row[CV]]] {
      override def run[T](f: Iterator[Row[CV]] => T): T =
        using(new ResourceScope) { rs =>
          f(allRows(limit, offset, sorted, rowId, rs))
        }
    }
}
