package com.socrata.datacoordinator
package truth.loader

import com.rojoma.simplearm.Managed

trait DatasetExtractor[CV] {
  def allRows(limit: Option[Long], offset: Option[Long], sorted: Boolean): Managed[Iterator[Row[CV]]]
}
