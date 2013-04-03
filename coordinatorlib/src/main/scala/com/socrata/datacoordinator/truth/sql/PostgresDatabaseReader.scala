package com.socrata.datacoordinator.truth
package sql

import com.rojoma.simplearm.Managed
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.metadata.{DatasetMapReader, CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.util.collection.{MutableColumnIdMap, ColumnIdMap}
import com.socrata.datacoordinator.id.ColumnId
import javax.sql.DataSource
import java.sql.{ResultSet, Connection}
import com.rojoma.simplearm.SimpleArm
import com.socrata.datacoordinator.truth.loader.sql.RepBasedDatasetExtractor

// Does this need to be *Postgres*, or is all postgres-specific stuff encapsulated in its paramters?
class PostgresDatabaseReader[CT, CV](conn: Connection,
                                     datasetMap: DatasetMapReader,
                                     repFor: ColumnInfo => SqlColumnReadRep[CT, CV])
  extends LowLevelDatabaseReader[CV]
{
  private class S(conn: Connection) extends ReadContext {
    val datasetMap = PostgresDatabaseReader.this.datasetMap

    def loadDataset(datasetName: String, copySelector: CopySelector): Option[(CopyInfo, ColumnIdMap[ColumnInfo])] = {
      val map = datasetMap
      for {
        datasetId <- map.datasetId(datasetName)
        datasetInfo <- map.datasetInfo(datasetId)
        copyInfo <- copySelector match {
          case LatestCopy => Some(map.latest(datasetInfo))
          case PublishedCopy => map.published(datasetInfo)
          case WorkingCopy => map.unpublished(datasetInfo)
          case Snapshot(n) => map.snapshot(datasetInfo, n)
        }
      } yield (copyInfo, map.schema(copyInfo))
    }

    def withRows[A](ci: CopyInfo, sidCol: ColumnInfo, schema: ColumnIdMap[ColumnInfo], f: (Iterator[ColumnIdMap[CV]]) => A, limit: Option[Long], offset: Option[Long]): A =
      new RepBasedDatasetExtractor(conn, ci.dataTableName, repFor(sidCol).asPKableRep, schema.mapValuesStrict(repFor)).allRows(limit, offset).map(f)
  }

  def openDatabase: Managed[ReadContext] = new SimpleArm[ReadContext] {
    def flatMap[A](f: ReadContext => A): A =
      f(new S(conn))
  }
}
