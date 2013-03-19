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
import org.slf4j.LoggerFactory
import com.socrata.datacoordinator.util.TimingReport

class PlaybackToSecondary[CT, CV](conn: Connection, secondaryManifest: SecondaryManifest, repFor: ColumnInfo => SqlColumnReadRep[CT, CV], timingReport: TimingReport) {
  require(!conn.getAutoCommit, "Connection must not be in auto-commit mode")

  val log = LoggerFactory.getLogger(classOf[PlaybackToSecondary[_,_]])
  val datasetMapReader = new PostgresDatasetMapReader(conn)

  val datasetLock = NoopDatasetLock
  val datasetLockTimeout = Duration.Inf

  def apply(datasetId: DatasetId, secondary: NamedSecondary[CV], datasetMapReader: DatasetMapReader, delogger: Delogger[CV]) {
    datasetMapReader.datasetInfo(datasetId) match {
      case Some(datasetInfo) =>
        log.info("Found dataset " + datasetInfo.datasetName + " in truth")
        try {
          if(secondary.store.wantsWorkingCopies) {
            log.info("Secondary store wants working copies")
            playbackAll(datasetInfo, secondary, datasetMapReader, delogger)
          } else {
            log.info("Secondary store wants doesn't want working copies")
            playbackPublished(datasetInfo, secondary, datasetMapReader, delogger)
          }
        } catch {
          case _: ResyncException =>
            log.info("Incremental update requested full resync")
            resync(datasetId, secondary, delogger)
        }
      case None =>
        log.info("Didn't find the dataset in truth")
        drop(secondary, datasetId)
    }
  }

  def drop(secondary: NamedSecondary[CV], datasetId: DatasetId) {
    timingReport("drop", "dataset" -> datasetId) {
      secondary.store.dropDataset(datasetId, getCookie(secondary, datasetId))
      dropFromSecondaryMap(secondary, datasetId)
    }
  }

  def resync(datasetId: DatasetId, secondary: NamedSecondary[CV], delogger: Delogger[CV]) {
    timingReport("resync", "dataset" -> datasetId) {
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
              timingReport("copy", "number" -> copy.copyNumber) {
                currentCookie =
                  if(copy.lifecycleStage == LifecycleStage.Discarded) secondary.store.dropCopy(datasetId, copy.copyNumber, currentCookie)
                  else if(copy.lifecycleStage != LifecycleStage.Unpublished) syncCopy(secondary, copy, w.schema(copy), currentCookie)
                  else if(secondary.store.wantsWorkingCopies) syncCopy(secondary, copy, w.schema(copy), currentCookie)
                  else currentCookie
              }
            }
            updateSecondaryMap(secondary, datasetInfo.systemId, newLastDataVersion, currentCookie)
          case None =>
            drop(secondary, datasetId)
        }
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
    timingReport("sync-copy", "secondary" -> secondary.storeId, "dataset" -> copy.datasetInfo.systemId, "copy" -> copy.copyNumber) {
      secondary.store.resync(copy, cookie, schema, new SimpleArm[Iterator[Row[CV]]] {
        def flatMap[A](f: Iterator[Row[CV]] => A): A =
          new RepBasedDatasetExtractor(conn, copy.dataTableName, schema.mapValuesStrict(repFor)).allRows.map(f)
      })
    }

  def playbackAll(datasetInfo: DatasetInfo, secondary: NamedSecondary[CV], datasetMapReader: DatasetMapReader, delogger: Delogger[CV]) {
    timingReport("playback-all", "secondary" -> secondary.storeId, "dataset" -> datasetInfo.systemId) {
      val latest = datasetMapReader.latest(datasetInfo)
      var currentCookie = getCookie(secondary, datasetInfo.systemId)
      val currentVersion = secondary.store.currentVersion(datasetInfo.systemId, currentCookie)

      if(latest.dataVersion > currentVersion) { // ok, we certainly need to do SOMETHING
        for(v <- (currentVersion + 1) to latest.dataVersion) {
          timingReport("version", "version" -> v) {
            using(delogger.delog(v)) { it =>
              if(!it.hasNext) throw new ResyncException // oops, there is no "next version"?
              currentCookie = playback(secondary, datasetInfo, v, it, currentCookie)
            }
          }
        }
      } else if(latest.dataVersion < currentVersion) {
        throw new ResyncException
      }
    }
  }

  def playback(secondary: NamedSecondary[CV], datasetInfo: DatasetInfo, dataVersion: Long, events: Iterator[Delogger.LogEvent[CV]], cookie: Secondary.Cookie): Secondary.Cookie = {
    timingReport("playback", "secondary" -> secondary.storeId, "dataset" -> datasetInfo.systemId, "version" -> dataVersion) {
      val newCookie = secondary.store.version(datasetInfo.systemId, dataVersion, cookie, events)
      updateSecondaryMap(secondary, datasetInfo.systemId, dataVersion, newCookie)
      newCookie
    }
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
    timingReport("playback-published", "secondary" -> secondary.storeId, "dataset" -> datasetInfo.systemId) {
      var currentCookie = getCookie(secondary, datasetInfo.systemId)
      val currentVersion = secondary.store.currentVersion(datasetInfo.systemId, currentCookie)
      log.info("Secondary store currently has {}", currentVersion)
      datasetMapReader.published(datasetInfo) match {
        case Some(publishedVersion) =>
          log.info("Copy number {} is currently published.  It has data version {}", publishedVersion.copyNumber, publishedVersion.dataVersion)
          if(publishedVersion.dataVersion > currentVersion) {
            log.info("Truth is newer than the secondary")

            if(delogger.findPublishEvent(currentVersion + 1, publishedVersion.dataVersion).nonEmpty) {
              log.info("Found a publish event in between the store's version and truth's; resyncing")
              throw new ResyncException
            }

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
                    playback(secondary, datasetInfo, v, it, currentCookie)
                  }
                }
              }
            }
            assert(!inWorkingCopy)
          } else if(publishedVersion.dataVersion < currentVersion) {
            log.info("Truth is older than the secondary?")
            throw new ResyncException
          } else {
            log.info("Truth and secondary are at the same version")
          }
        case None =>
          log.info("No published version exists")
      }
    }
  }

  private class ResyncException extends ControlThrowable
}
