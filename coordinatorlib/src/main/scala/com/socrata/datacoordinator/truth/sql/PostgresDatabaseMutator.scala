package com.socrata.datacoordinator.truth
package sql

import java.sql.Connection
import javax.sql.DataSource

import org.joda.time.DateTime
import com.rojoma.simplearm.v2._

import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.truth.loader.sql.{RepBasedSqlDatasetContentsCopier, SqlLogger}
import com.socrata.datacoordinator.truth.{TypeContext, RowLogCodec}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.id.{DatasetId, RowId}
import scala.concurrent.duration.Duration
import com.socrata.datacoordinator.util.{RowIdProvider, RowVersionProvider, RowDataProvider, TimingReport}
import com.socrata.datacoordinator.truth.universe._
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.metadata.DatasetInfo
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.truth.metadata.DatasetInfo
import com.socrata.datacoordinator.truth.metadata.CopyInfo

// Does this need to be *Postgres*, or is all postgres-specific stuff encapsulated in its paramters?
// Actually does this need to be in the sql package at all now that Universe exists?
class PostgresDatabaseMutator[CT, CV](universe: Managed[Universe[CT, CV] with LoggerProvider with SchemaLoaderProvider with LoaderProvider with TruncatorProvider with DatasetContentsCopierProvider with DatasetMapWriterProvider with SecondaryManifestProvider])
  extends LowLevelDatabaseMutator[CT, CV]
{
  // type LoaderProvider = (CopyInfo, ColumnIdMap[ColumnInfo], RowPreparer[CV], IdProvider, Logger[CV], ColumnInfo => SqlColumnRep[CT, CV]) => Loader[CV]

  private class S(val universe: Universe[CT, CV] with LoggerProvider with SchemaLoaderProvider with LoaderProvider with TruncatorProvider with DatasetContentsCopierProvider with DatasetMapWriterProvider with SecondaryManifestProvider) extends MutationContext {
    lazy val now = universe.transactionStart

    final def loadLatestVersionOfDataset(datasetId: DatasetId, lockTimeout: Duration): Option[DatasetCopyContext[CT]] = {
      val map = datasetMap
      map.datasetInfo(datasetId, lockTimeout) map { datasetInfo =>
        val latest = map.latest(datasetInfo)
        val schema = map.schema(latest)
        new DatasetCopyContext(latest, schema)
      }
    }

    def outOfDateFeedbackSecondaries(datasetId: DatasetId): Set[String] =
      universe.secondaryManifest.outOfDateFeedbackSecondaries(datasetId)

    def logger(datasetInfo: DatasetInfo, user: String): Logger[CT, CV] =
      universe.logger(datasetInfo, user)

    def schemaLoader(logger: Logger[CT, CV]): SchemaLoader[CT] =
      universe.schemaLoader(logger)

    def truncate(table: CopyInfo, logger: Logger[CT, CV]) =
      universe.truncator.truncate(table, logger)

    def datasetContentsCopier(logger: Logger[CT, CV]): DatasetContentsCopier[CT] =
      universe.datasetContentsCopier(logger)

    def finishDatasetTransaction(username: String, copyInfo: CopyInfo, updateLastUpdated: Boolean) {
      val dsLogger = logger(copyInfo.datasetInfo, username)

      val updatedCopyInfo =
        if(updateLastUpdated) {
          val newCopyInfo = datasetMap.updateLastModified(copyInfo)
          dsLogger.lastModifiedChanged(newCopyInfo.lastModified)
          newCopyInfo
        } else {
          copyInfo
        }

      dsLogger.endTransaction() foreach { ver =>
        datasetMap.updateDataVersion(updatedCopyInfo, ver)
      }
    }

    def datasetMap = universe.datasetMapWriter

    def withDataLoader[A](copyCtx: DatasetCopyContext[CT], logger: Logger[CT, CV], reportWriter: ReportWriter[CV], replaceUpdatedRows: Boolean, updateOnly: Boolean)(f: (Loader[CV]) => A): (Long, A) = {
      val dataProvider = new RowDataProvider(copyCtx.datasetInfo.nextCounterValue)
      for(loader <- universe.loader(copyCtx, new RowIdProvider(dataProvider), new RowVersionProvider(dataProvider), logger, reportWriter, replaceUpdatedRows, updateOnly)) {
        val result = f(loader)
        loader.finish()
        (dataProvider.finish(), result)
      }
    }

    def secondaryReindex(logger: Logger[CT, CV]) = {}
  }

  def openDatabase: Managed[MutationContext] = new Managed[MutationContext] {
    def run[A](f: MutationContext => A): A =
      for { u <- universe } f(new S(u))
  }
}
