package com.socrata.datacoordinator
package truth.loader

import com.rojoma.simplearm.v2._

trait DatasetExtractor[CV] {
  def allRows(limit: Option[Long], offset: Option[Long], sorted: Boolean, rowId: Option[CV]): Managed[Iterator[Row[CV]]]
}
