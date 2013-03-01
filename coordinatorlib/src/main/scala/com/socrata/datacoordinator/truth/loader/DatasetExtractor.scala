package com.socrata.datacoordinator
package truth.loader

import com.rojoma.simplearm.Managed

trait DatasetExtractor[CV] {
  def allRows: Managed[Iterator[Row[CV]]]
}
