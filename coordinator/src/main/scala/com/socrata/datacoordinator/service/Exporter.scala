package com.socrata.datacoordinator
package service

import com.socrata.datacoordinator.truth.universe.{DatasetReaderProvider, Universe}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.ColumnInfo

object Exporter {
  def export[CT, CV, T](u: Universe[CT, CV] with DatasetReaderProvider, id: String, columns: Option[Set[String]])(f: (ColumnIdMap[ColumnInfo], Iterator[Row[CV]]) => T): Option[T] = {
    for {
      ctxOpt <- u.datasetReader.openDataset(id, latest = true)
      ctx <- ctxOpt
    } yield {
      import ctx. _

      val selectedSchema = columns match {
        case Some(set) => schema.filter { case (_, ci) => set(ci.logicalName) }
        case None => schema
      }

      withRows(selectedSchema.keySet) { it =>
        f(selectedSchema, it)
      }
    }
  }
}
