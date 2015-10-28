package com.socrata.datacoordinator
package truth.loader

import java.io.Writer
import com.socrata.datacoordinator.id.ColumnId

trait DatasetCsvifier[CV] {
  /** Writes a CSVified form of the dataset to the target `Writer`,
    * in the order specified by the `columns` constructor.
    *
    * Note: the CSV produced is slightly quirky.  In order to represent `null`,
    * an empty unquoted cell is used.  In order to represent the empty string,
    * `""` is used.  This is consistent with the way postgresql imports CSV
    * files (and in fact this CSV can be `COPY`'d into a postgresql table
    * unmanipulated). */
  def csvify(target: Writer, columns: Seq[ColumnId]): Unit
}
