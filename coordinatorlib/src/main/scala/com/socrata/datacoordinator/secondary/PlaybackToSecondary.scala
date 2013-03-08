package com.socrata.datacoordinator
package secondary

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.loader.Delogger
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.metadata._
import scala.util.control.ControlThrowable
import java.sql.Connection
import com.socrata.datacoordinator.truth.loader.sql.RepBasedDatasetExtractor
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresDatasetMapWriter, PostgresDatasetMapReader}
import com.socrata.datacoordinator.truth.NoopDatasetLock
import scala.concurrent.duration.Duration
import scala.Some
import com.socrata.datacoordinator.truth.metadata.DatasetInfo
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.rojoma.simplearm.SimpleArm
import com.socrata.datacoordinator.truth.sql.SqlColumnReadRep

class PlaybackToSecondary[CT, CV](conn: Connection, secondaryManifest: SecondaryManifest, repFor: ColumnInfo => SqlColumnReadRep[CT, CV]) {
  val datasetMapReader = new PostgresDatasetMapReader(conn)

  val datasetLock = NoopDatasetLock
  val datasetLockTimeout = Duration.Inf

  def apply(datasetId: DatasetId, secondary: NamedSecondary[CV], datasetMapReader: DatasetMapReader, delogger: Delogger[CV]) {
    datasetMapReader.datasetInfo(datasetId) match {
      case Some(datasetInfo) =>
        try {
          if(secondary.store.wantsWorkingCopies) {
            playbackAll(datasetInfo, secondary, datasetMapReader, delogger)
          } else {
            playbackPublished(datasetInfo, secondary, datasetMapReader, delogger)
          }
        } catch {
          case _: ResyncException =>
            resync(datasetId, secondary, delogger)
        }
      case None =>
        drop(secondary, datasetId)
    }
  }

  def drop(secondary: NamedSecondary[CV], datasetId: DatasetId) {
    secondary.store.dropDataset(datasetId, getCookie(secondary, datasetId))
    dropFromSecondaryMap(secondary, datasetId)
  }

  def resync(datasetId: DatasetId, secondary: NamedSecondary[CV], delogger: Delogger[CV]) {
    datasetLock.withDatasetLock(datasetId, datasetLockTimeout) {
      val w = new PostgresDatasetMapWriter(conn)
      w.datasetInfo(datasetId) match {
        case Some(datasetInfo) =>
          val allCopies = w.allCopies(datasetInfo)
          var currentCookie = getCookie(secondary, datasetId)
          val newLastDataVersion =
            if(secondary.store.wantsWorkingCopies) delogger.lastVersion.getOrElse(0L)
            else lastPublishedDataVersion(delogger)
          for(copy <- allCopies) {
            currentCookie =
              if(copy.lifecycleStage == LifecycleStage.Discarded) secondary.store.dropCopy(datasetId, copy.copyNumber, currentCookie)
              else if(copy.lifecycleStage != LifecycleStage.Unpublished) syncCopy(secondary, copy, w.schema(copy), currentCookie)
              else if(secondary.store.wantsWorkingCopies) syncCopy(secondary, copy, w.schema(copy), currentCookie)
              else currentCookie
          }
          updateSecondaryMap(secondary, datasetInfo.systemId, newLastDataVersion, currentCookie)
        case None =>
          drop(secondary, datasetId)
      }
    }
  }

  def lastPublishedDataVersion(delogger: Delogger[CV]) = {
    val lastCopyCreatedVersion = delogger.lastWorkingCopyCreatedVersion.getOrElse(0L)
    val lastCopyDiscardedOrPromotedVersion = delogger.lastWorkingCopyDroppedOrPublishedVersion.getOrElse(0L)
    if(lastCopyDiscardedOrPromotedVersion > lastCopyCreatedVersion) {
      delogger.lastVersion.getOrElse(0L)
    } else {
      lastCopyCreatedVersion - 1
    }
  }

  def syncCopy(secondary: NamedSecondary[CV], copy: CopyInfo, schema: ColumnIdMap[ColumnInfo], cookie: Secondary.Cookie): Secondary.Cookie =
    secondary.store.resync(copy, cookie, schema, new SimpleArm[Iterator[Row[CV]]] {
      def flatMap[A](f: Iterator[Row[CV]] => A): A =
        new RepBasedDatasetExtractor(conn, copy.dataTableName, schema.mapValuesStrict(repFor)).allRows.map(f)
    })

  def playbackAll(datasetInfo: DatasetInfo, secondary: NamedSecondary[CV], datasetMapReader: DatasetMapReader, delogger: Delogger[CV]) {
    val latest = datasetMapReader.latest(datasetInfo)
    var currentCookie = getCookie(secondary, datasetInfo.systemId)
    val currentVersion = secondary.store.currentVersion(datasetInfo.systemId, currentCookie)

    if(latest.dataVersion > currentVersion) { // ok, we certainly need to do SOMETHING
      for(v <- (currentVersion + 1) to latest.dataVersion) {
        using(delogger.delog(v)) { it =>
          if(!it.hasNext) throw new ResyncException // oops, there is no "next version"?
          currentCookie = playback(secondary, datasetInfo, v, it, currentCookie)
        }
      }
    } else if(latest.dataVersion < currentVersion) {
      throw new ResyncException
    }
  }

  def playback(secondary: NamedSecondary[CV], datasetInfo: DatasetInfo, dataVersion: Long, events: Iterator[Delogger.LogEvent[CV]], cookie: Secondary.Cookie): Secondary.Cookie = {
    val newCookie = secondary.store.version(datasetInfo.systemId, dataVersion, cookie, events)
    updateSecondaryMap(secondary, datasetInfo.systemId, dataVersion, newCookie)
    newCookie
  }

  def getCookie(secondary: NamedSecondary[CV], datasetId: DatasetId): Secondary.Cookie =
    secondaryManifest.lastDataInfo(secondary.storeId, datasetId)._2

  def updateSecondaryMap(secondary: NamedSecondary[CV], datasetId: DatasetId, newLastDataVersion: Long, newCookie: Secondary.Cookie) {
    secondaryManifest.updateDataInfo(secondary.storeId, datasetId, newLastDataVersion, newCookie)
  }

  def dropFromSecondaryMap(secondary: NamedSecondary[CV], datasetId: DatasetId) {
    secondaryManifest.dropDataset(secondary.storeId, datasetId)
  }

  def playbackPublished(datasetInfo: DatasetInfo, secondary: NamedSecondary[CV], datasetMapReader: DatasetMapReader, delogger: Delogger[CV]) {
    var currentCookie = getCookie(secondary, datasetInfo.systemId)
    val currentVersion = secondary.store.currentVersion(datasetInfo.systemId, currentCookie)
    datasetMapReader.published(datasetInfo) match {
      case Some(publishedVersion) =>
        if(publishedVersion.dataVersion > currentVersion) {
          if(delogger.findPublishEvent(currentVersion + 1, publishedVersion.dataVersion).nonEmpty)
            throw new ResyncException

          var inWorkingCopy = false
          for(v <- (currentVersion + 1) to publishedVersion.dataVersion) {
            currentCookie = using(delogger.delog(v)) { itRaw =>
              if(!itRaw.hasNext) throw new ResyncException // oops, there is no "next version"?
              val it = itRaw.buffered
              if(inWorkingCopy) {
                if(it.head.companion == Delogger.WorkingCopyDropped) {
                  inWorkingCopy = false
                  it.next() // skip the drop
                  playback(secondary, datasetInfo, v, it, currentCookie)
                } else {
                  assert(it.head.companion != Delogger.WorkingCopyPublished)
                  playback(secondary, datasetInfo, v, Iterator.empty, currentCookie)
                }
              } else {
                if(it.head.companion == Delogger.WorkingCopyCreated) {
                  inWorkingCopy = true
                  playback(secondary, datasetInfo, v, Iterator.empty, currentCookie)
                } else {
                  playback(secondary, datasetInfo, v, Iterator.empty, currentCookie)
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
