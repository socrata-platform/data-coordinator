package com.socrata.datacoordinator
package secondary

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.loader.Delogger
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.metadata._
import scala.util.control.ControlThrowable
import java.sql.Connection
import com.socrata.datacoordinator.truth.loader.sql.{RepBasedDatasetExtractor, PostgresDatasetDecsvifier}
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresDatasetMapWriter, PostgresDatasetMapReader}
import com.socrata.datacoordinator.truth.{DatasetReader, NoopDatasetLock}
import scala.concurrent.duration.Duration
import scala.Some
import com.socrata.datacoordinator.truth.metadata.DatasetInfo
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.rojoma.simplearm.SimpleArm
import com.socrata.datacoordinator.truth.sql.{SqlColumnReadRep, PostgresDatabaseReader}

class PlaybackToSecondary[CT, CV](conn: Connection, repFor: ColumnInfo => SqlColumnReadRep[CT, CV]) {
  val datasetMapReader = new PostgresDatasetMapReader(conn)

  val datasetLock = NoopDatasetLock
  val datasetLockTimeout = Duration.Inf

  def apply(datasetId: DatasetId, secondary: Secondary[CV], datasetMapReader: DatasetMapReader, delogger: Delogger[CV]) {
    datasetMapReader.datasetInfo(datasetId) match {
      case Some(datasetInfo) =>
        try {
          if(secondary.wantsWorkingCopies) {
            playbackAll(datasetInfo, secondary, datasetMapReader, delogger)
          } else {
            playbackPublished(datasetInfo, secondary, datasetMapReader, delogger)
          }
        } catch {
          case _: ResyncException =>
            resync(datasetId, secondary)
        }
      case None =>
        secondary.dropDataset(datasetId)
    }
  }

  def resync(datasetId: DatasetId, secondary: Secondary[CV]) {
    datasetLock.withDatasetLock(datasetId, datasetLockTimeout) {
      val w = new PostgresDatasetMapWriter(conn)
      w.datasetInfo(datasetId) match {
        case Some(datasetInfo) =>
          val allCopies = w.allCopies(datasetInfo)
          for(copy <- allCopies) {
            if(copy.lifecycleStage == LifecycleStage.Discarded) secondary.dropCopy(datasetId, copy.copyNumber)
            else if(copy.lifecycleStage != LifecycleStage.Unpublished) syncCopy(secondary, copy, w.schema(copy))
            else if(secondary.wantsWorkingCopies) syncCopy(secondary, copy, w.schema(copy))
          }
        case None =>
          secondary.dropDataset(datasetId)
      }
    }
  }

  def syncCopy(secondary: Secondary[CV], copy: CopyInfo, schema: ColumnIdMap[ColumnInfo]) =
    secondary.resync(copy, schema, new SimpleArm[Iterator[Row[CV]]] {
      def flatMap[A](f: Iterator[Row[CV]] => A): A =
        new RepBasedDatasetExtractor(conn, copy.dataTableName, schema.mapValuesStrict(repFor)).allRows.map(f)
    })

  def playbackAll(datasetInfo: DatasetInfo, secondary: Secondary[CV], datasetMapReader: DatasetMapReader, delogger: Delogger[CV]) {
    val latest = datasetMapReader.latest(datasetInfo)
    val currentVersion = secondary.currentVersion(datasetInfo.systemId)

    if(latest.dataVersion > currentVersion) { // ok, we certainly need to do SOMETHING
      for(v <- (currentVersion + 1) to latest.dataVersion) {
        using(delogger.delog(v)) { it =>
          if(!it.hasNext) throw new ResyncException // oops, there is no "next version"?
          playback(secondary, datasetInfo, v, it)
        }
      }
    } else if(latest.dataVersion < currentVersion) {
      throw new ResyncException
    }
  }

  def playback(secondary: Secondary[CV], datasetInfo: DatasetInfo, dataVersion: Long, events: Iterator[Delogger.LogEvent[CV]]) {
    secondary.version(datasetInfo.systemId, dataVersion, events)
  }

  def playbackPublished(datasetInfo: DatasetInfo, secondary: Secondary[CV], datasetMapReader: DatasetMapReader, delogger: Delogger[CV]) {
    val currentVersion = secondary.currentVersion(datasetInfo.systemId)
    datasetMapReader.published(datasetInfo) match {
      case Some(publishedVersion) =>
        if(publishedVersion.dataVersion > currentVersion) {
          if(delogger.findPublishEvent(currentVersion + 1, publishedVersion.dataVersion).nonEmpty)
            throw new ResyncException

          var inWorkingCopy = false
          for(v <- (currentVersion + 1) to publishedVersion.dataVersion) {
            using(delogger.delog(v)) { itRaw =>
              if(!itRaw.hasNext) throw new ResyncException // oops, there is no "next version"?
              val it = itRaw.buffered
              if(inWorkingCopy) {
                if(it.head.companion == Delogger.WorkingCopyDropped) {
                  inWorkingCopy = false
                  it.next() // skip the drop
                  playback(secondary, datasetInfo, v, it)
                } else {
                  assert(it.head.companion != Delogger.WorkingCopyPublished)
                  playback(secondary, datasetInfo, v, Iterator.empty)
                }
              } else {
                if(it.head.companion == Delogger.WorkingCopyCreated) {
                  inWorkingCopy = true
                  playback(secondary, datasetInfo, v, Iterator.empty)
                } else {
                  playback(secondary, datasetInfo, v, Iterator.empty)
                }
              }
            }
          }
          assert(!inWorkingCopy)
        } else if(publishedVersion.dataVersion < currentVersion) {
          throw new ResyncException
        }
      case None =>
        // ok.  Hasn't been published yet for the first time...
    }
  }

  private class ResyncException extends ControlThrowable
}
