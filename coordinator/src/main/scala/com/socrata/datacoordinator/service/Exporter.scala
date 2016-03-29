package com.socrata.datacoordinator
package service

import com.socrata.datacoordinator.id.{UserColumnId, DatasetId}
import com.socrata.datacoordinator.truth.CopySelector
import com.socrata.datacoordinator.truth.metadata.{DatasetCopyContext, CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.truth.universe.{SchemaFinderProvider, CacheProvider}
import com.socrata.datacoordinator.truth.universe.{DatasetReaderProvider, Universe}
import com.socrata.datacoordinator.util.collection.{UserColumnIdSet, MutableColumnIdMap, ColumnIdMap}
import com.socrata.http.server.util.{NoPrecondition, StrongEntityTag, EntityTag, Precondition}
import com.socrata.soql.environment.ColumnName
import java.nio.charset.StandardCharsets
import org.joda.time.DateTime

object Exporter {
  sealed abstract class Result[+T]
  case object NotFound extends Result[Nothing]
  case object PreconditionFailedBecauseNoMatch extends Result[Nothing]
  case class NotModified(etags: Seq[EntityTag]) extends Result[Nothing]
  case object InvalidRowId extends Result[Nothing]
  case class Success[T](x: T) extends Result[T]

  def export[CT, CV, T](u: Universe[CT, CV] with DatasetReaderProvider,
                        id: DatasetId,
                        copy: CopySelector,
                        columns: Option[UserColumnIdSet],
                        limit: Option[Long],
                        offset: Option[Long],
                        precondition: Precondition,
                        ifModifiedSince: Option[DateTime],
                        sorted: Boolean,
                        rowId: Option[CV])
                       (f: (EntityTag, DatasetCopyContext[CT], Long, Iterator[Row[CV]]) => T): Result[T] = {
    val subResult = for {
      ctxOpt <- u.datasetReader.openDataset(id, copy)
      ctx <- ctxOpt
    } yield {
      import ctx._

      val entityTag = StrongEntityTag(copyInfo.dataVersion.toString.getBytes(StandardCharsets.UTF_8))
      precondition.check(Some(entityTag), sideEffectFree = true) match {
        case Precondition.Passed =>
          ifModifiedSince match {
            case Some(ims) if !copyInfo.lastModified.minusMillis(copyInfo.lastModified.getMillisOfSecond).isAfter(ims)
                              && precondition.equals(NoPrecondition) =>
              NotModified(Seq(entityTag))
            case _ =>
              val selectedSchema = columns match {
                case Some(set) => schema.filter { case (_, ci) => set(ci.userColumnId) }
                case None => schema
              }
              val properSchema = (copyCtx.userIdCol ++
                                  copyCtx.systemIdCol).foldLeft(selectedSchema) { (ss, idCol) =>
                ss + (idCol.systemId -> idCol)
              }

              for(it <- rows(properSchema.keySet, limit = limit, offset = offset, sorted = sorted, rowId)) yield {
                val toRemove = properSchema.keySet -- selectedSchema.keySet
                if(toRemove.isEmpty) it
                else it.map { row =>
                  val m = new MutableColumnIdMap(row)
                  toRemove.foreach(m -= _)
                  m.freeze()
                }
                Success(f(entityTag, copyCtx.verticalSlice { ci => selectedSchema.keySet(ci.systemId) },
                  approximateRowCount,
                  if(selectedSchema.contains(copyCtx.pkCol_!.systemId)) it
                  else it.map(_ - copyCtx.pkCol_!.systemId)))
              }
          }
        case Precondition.FailedBecauseMatch(etags) =>
          NotModified(etags)
        case Precondition.FailedBecauseNoMatch =>
          PreconditionFailedBecauseNoMatch
      }
    }

    subResult.getOrElse(NotFound)
  }
}
