package com.socrata.datacoordinator
package service

import com.socrata.datacoordinator.truth.universe.{DatasetReaderProvider, Universe}
import com.socrata.datacoordinator.util.collection.{UserColumnIdSet, MutableColumnIdMap, ColumnIdMap}
import com.socrata.datacoordinator.truth.metadata.{DatasetCopyContext, CopyInfo, ColumnInfo}
import com.socrata.soql.environment.ColumnName
import com.socrata.datacoordinator.truth.CopySelector
import com.socrata.datacoordinator.id.{UserColumnId, DatasetId}

object Exporter {
  def export[CT, CV, T](u: Universe[CT, CV] with DatasetReaderProvider, id: DatasetId, copy: CopySelector, columns: Option[UserColumnIdSet], limit: Option[Long], offset: Option[Long])(f: (DatasetCopyContext[CT], Iterator[Row[CV]]) => T): Option[T] = {
    for {
      ctxOpt <- u.datasetReader.openDataset(id, copy)
      ctx <- ctxOpt
    } yield {
      import ctx._

      val selectedSchema = columns match {
        case Some(set) => schema.filter { case (_, ci) => set(ci.userColumnId) }
        case None => schema
      }
      val properSchema = (copyCtx.userIdCol ++ copyCtx.systemIdCol).foldLeft(selectedSchema) { (ss, idCol) =>
        ss + (idCol.systemId -> idCol)
      }

      for(it <- rows(properSchema.keySet, limit = limit, offset = offset)) yield {
        val toRemove = properSchema.keySet -- selectedSchema.keySet
        if(toRemove.isEmpty) it
        else it.map { row =>
          val m = new MutableColumnIdMap(row)
          toRemove.foreach(m -= _)
          m.freeze()
        }
        f(copyCtx.verticalSlice { ci => selectedSchema.keySet(ci.systemId) },
          if(selectedSchema.contains(copyCtx.pkCol_!.systemId)) it
          else it.map(_ - copyCtx.pkCol_!.systemId))
      }
    }
  }
}
