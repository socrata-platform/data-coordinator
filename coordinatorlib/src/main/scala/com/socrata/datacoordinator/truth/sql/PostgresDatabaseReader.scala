package com.socrata.datacoordinator.truth
package sql

import com.rojoma.simplearm.v2._

import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.util.CopyContextResult
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.id.{DatasetId, ColumnId}
import java.sql.Connection
import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.truth.loader.sql.RepBasedDatasetExtractor
import com.socrata.datacoordinator.truth.Snapshot
import scala.Some

// Does this need to be *Postgres*, or is all postgres-specific stuff encapsulated in its paramters?
class PostgresDatabaseReader[CT, CV](conn: Connection,
                                     datasetMap: DatasetMapReader[CT],
                                     repFor: ColumnInfo[CT] => SqlColumnReadRep[CT, CV])
  extends LowLevelDatabaseReader[CT, CV]
{
  private class S(conn: Connection) extends ReadContext {
    val datasetMap = PostgresDatabaseReader.this.datasetMap

    def loadDataset(datasetId: DatasetId, copySelector: CopySelector): CopyContextResult[DatasetCopyContext[CT]] = {
      val map = datasetMap
      val datasetInfo = map.datasetInfo(datasetId).getOrElse { return CopyContextResult.NoSuchDataset }
      val copyInfo = copySelector match {
        case LatestCopy => Some(map.latest(datasetInfo))
        case PublishedCopy => map.published(datasetInfo)
        case WorkingCopy => map.unpublished(datasetInfo)
        case Snapshot(n) => map.snapshot(datasetInfo, n)
      }
      copyInfo match {
        case Some(copyInfo) => CopyContextResult.CopyInfo(loadDataset(copyInfo))
        case None => CopyContextResult.NoSuchCopy
      }
    }

    def loadDataset(copyInfo: CopyInfo) =
      new DatasetCopyContext(copyInfo, datasetMap.schema(copyInfo))

    def approximateRowCount(copyCtx: DatasetCopyContext[CT]): Long = {
      val approx =
        using(conn.prepareStatement("SELECT reltuples FROM pg_class WHERE relname=?")) { stmt =>
          stmt.setString(1, copyCtx.copyInfo.dataTableName)
          using(stmt.executeQuery()) { rs =>
            if(rs.next()) {
              rs.getLong("reltuples")
            } else {
              -1L
            }
          }
        }
      if(approx <= 1000) { // small enough we can take the hit to get an exact number
        val colRep = repFor(copyCtx.systemIdCol_!).asPKableRep
        for {
          stmt <- managed(conn.prepareStatement("SELECT " + colRep.count + " FROM " + copyCtx.copyInfo.dataTableName))
          rs <- managed(stmt.executeQuery())
        } {
          val foundOne = rs.next()
          assert(foundOne, "select count(id) returned zero rows?")
          rs.getLong(1)
        }
      } else {
        approx
      }
    }

    def rows(copyCtx: DatasetCopyContext[CT],
             sidCol: ColumnId,
             limit: Option[Long],
             offset: Option[Long],
             sorted: Boolean,
             rowId: Option[CV] = None): Managed[Iterator[ColumnIdMap[CV]]] =
      new RepBasedDatasetExtractor(
        conn,
        copyCtx.copyInfo.dataTableName,
        repFor(copyCtx.schema(sidCol)).asPKableRep,
        copyCtx.schema.mapValuesStrict(repFor)).allRows(limit, offset, sorted, rowId)
  }

  def openDatabase: Managed[ReadContext] = new Managed[ReadContext] {
    def run[A](f: ReadContext => A): A =
      f(new S(conn))
  }
}
