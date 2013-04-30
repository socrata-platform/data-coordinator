package com.socrata.datacoordinator
package service

import com.socrata.datacoordinator.truth.universe.{DatasetReaderProvider, Universe}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.{DatasetCopyContext, CopyInfo, ColumnInfo}
import com.socrata.soql.environment.ColumnName
import com.socrata.datacoordinator.truth.CopySelector
import com.socrata.datacoordinator.id.DatasetId

object Exporter {
  def export[CT, CV, T](u: Universe[CT, CV] with DatasetReaderProvider, id: DatasetId, copy: CopySelector, columns: Option[Set[ColumnName]], limit: Option[Long], offset: Option[Long])(f: (DatasetCopyContext[CT], Iterator[Row[CV]]) => T): Option[T] = {
    for {
      ctxOpt <- u.datasetReader.openDataset(id, copy)
      ctx <- ctxOpt
    } yield {
      import ctx. _

      val selectedSchema = columns match {
        case Some(set) => schema.filter { case (_, ci) => set(ci.logicalName) }
        case None => schema
      }

      withRows(selectedSchema.keySet, limit, offset) { it =>
        f(copyCtx, it)
      }
    }
  }
}
