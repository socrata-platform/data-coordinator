package com.socrata.datacoordinator.truth.loader

import java.io.{OutputStream, Reader}
import com.socrata.datacoordinator.id.ColumnId

trait DatasetDecsvifier {
  def importFromCsv(output: OutputStream => Unit, columns: Seq[ColumnId]): Unit
}
