package com.socrata.datacoordinator.truth
package sql

import com.rojoma.simplearm.Managed
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.id.{DatasetId, ColumnId}
import java.sql.Connection
import com.rojoma.simplearm.SimpleArm
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

    def loadDataset(datasetId: DatasetId, copySelector: CopySelector): Option[DatasetCopyContext[CT]] = {
      val map = datasetMap
      for {
        datasetInfo <- map.datasetInfo(datasetId)
        copyInfo <- copySelector match {
          case LatestCopy => Some(map.latest(datasetInfo))
          case PublishedCopy => map.published(datasetInfo)
          case WorkingCopy => map.unpublished(datasetInfo)
          case Snapshot(n) => map.snapshot(datasetInfo, n)
        }
      } yield new DatasetCopyContext(copyInfo, map.schema(copyInfo))
    }

    def withRows[A](copyCtx: DatasetCopyContext[CT], sidCol: ColumnId, f: (Iterator[ColumnIdMap[CV]]) => A, limit: Option[Long], offset: Option[Long]): A =
      new RepBasedDatasetExtractor(conn, copyCtx.copyInfo.dataTableName, repFor(copyCtx.schema(sidCol)).asPKableRep, copyCtx.schema.mapValuesStrict(repFor)).allRows(limit, offset).map(f)
  }

  def openDatabase: Managed[ReadContext] = new SimpleArm[ReadContext] {
    def flatMap[A](f: ReadContext => A): A =
      f(new S(conn))
  }
}
