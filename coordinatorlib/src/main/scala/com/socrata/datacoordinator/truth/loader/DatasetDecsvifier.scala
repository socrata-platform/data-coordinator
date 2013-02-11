package com.socrata.datacoordinator.truth.loader

import java.io.Reader
import com.socrata.datacoordinator.id.ColumnId

trait DatasetDecsvifier[CV] {
  def importFromCsv(reader: Reader, columns: Seq[ColumnId])
}
