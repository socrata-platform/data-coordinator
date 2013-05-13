package com.socrata.datacoordinator
package secondary

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.loader.{MissingVersion, Delogger}
import com.socrata.datacoordinator.id.{RowId, DatasetId}
import com.socrata.datacoordinator.truth.metadata._
import scala.util.control.ControlThrowable
import java.sql.Connection
import com.socrata.datacoordinator.truth.loader.sql.RepBasedDatasetExtractor
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresDatasetMapWriter, PostgresDatasetMapReader}
import scala.concurrent.duration.Duration
import com.socrata.datacoordinator.truth.metadata.DatasetInfo
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.rojoma.simplearm.SimpleArm
import com.socrata.datacoordinator.truth.sql.SqlColumnReadRep
import org.slf4j.LoggerFactory
import com.socrata.datacoordinator.util.TimingReport
import com.socrata.datacoordinator.truth.loader.Delogger.{WorkingCopyPublished, WorkingCopyDropped, WorkingCopyCreated}

class PlaybackToSecondary[CT, CV](conn: Connection, secondaryManifest: SecondaryManifest, typeNamespace: TypeNamespace[CT], repFor: ColumnInfo[CT] => SqlColumnReadRep[CT, CV], datasetIdFormatter: DatasetId => String, timingReport: TimingReport) {
  require(!conn.getAutoCommit, "Connection must not be in auto-commit mode")

  val log = LoggerFactory.getLogger(classOf[PlaybackToSecondary[_,_]])
  val datasetMapReader = new PostgresDatasetMapReader[CT](conn, typeNamespace, timingReport)

  val datasetLockTimeout = Duration.Inf

  class LifecycleStageTrackingIterator(underlying: Iterator[Delogger.LogEvent[CV]], initialStage: LifecycleStage) extends BufferedIterator[Delogger.LogEvent[CV]] {
    private var currentStage = initialStage
    private var lookahead: Delogger.LogEvent[CV] = null

    def stageBeforeNextEvent = currentStage

    def stageAfterNextEvent = computeNextStage(head)

    def hasNext = lookahead != null || underlying.hasNext

    def head = {
      if(lookahead == null) lookahead = advance()
      lookahead
    }

    private def advance() = underlying.next()

    def next() = {
      val ev =
        if(lookahead == null) {
          advance()
        } else {
          val peeked = lookahead
          lookahead = null
          peeked
        }
      currentStage = computeNextStage(ev)
      ev
    }

    def finalLifecycleStage() = {
      while(hasNext) next()
      currentStage
    }

    private def computeNextStage(ev: Delogger.LogEvent[CV]) =
      ev match {
        case WorkingCopyCreated(_, _) =>
          LifecycleStage.Unpublished
        case WorkingCopyDropped | WorkingCopyPublished =>
          LifecycleStage.Published
        case _ =>
          currentStage
      }
  }

  // This is guaranteed to consume no more than necessary out of the iterator.
  // In particular, when it is done, the underlying iterator will either be empty
  // or positioned so that next() is a stage-changing event.
  class StageLimitedIterator(underlying: LifecycleStageTrackingIterator) extends Iterator[Delogger.LogEvent[CV]] {
    private val wantedStage = underlying.stageBeforeNextEvent

    def hasNext = underlying.hasNext && underlying.stageAfterNextEvent == wantedStage
    def next() =
      if(hasNext) underlying.next()
      else Iterator.empty.next()

    def finish() = while(hasNext) next()
  }


  def apply(datasetId: DatasetId, secondary: NamedSecondary[CT, CV], job: SecondaryRecord, delogger: Delogger[CV]) {
    val internalName = datasetIdFormatter(datasetId)
    var currentCookie = job.initialCookie
    var currentLifecycleStage = job.startingLifecycleStage
    datasetMapReader.datasetInfo(datasetId) match {
      case Some(datasetInfo) =>
        log.info("Found dataset " + datasetInfo.systemId + " in truth")
        try {
          (job.startingDataVersion to job.endingDataVersion).foreach { dataVersion =>
            using(delogger.delog(dataVersion)) { rawIt =>
              val it = new LifecycleStageTrackingIterator(rawIt, currentLifecycleStage)
              if(secondary.store.wantsWorkingCopies) {
                currentCookie = secondary.store.version(internalName, dataVersion, currentCookie, it)
              } else {
                while(it.hasNext) {
                  if(currentLifecycleStage != LifecycleStage.Published) {
                    // skip until it IS published, then resync
                    while(it.hasNext && it.stageAfterNextEvent != LifecycleStage.Published) it.next()
                    if(it.hasNext) throw new ResyncForPickySecondary
                  } else {
                    val publishedIt = new StageLimitedIterator(it)
                    if(publishedIt.hasNext) {
                      currentCookie = secondary.store.version(internalName, dataVersion, currentCookie, publishedIt)
                      publishedIt.finish()
                    }
                  }
                }
              }
              currentLifecycleStage = it.finalLifecycleStage()
              updateSecondaryMap(secondary, datasetId, dataVersion, currentLifecycleStage, currentCookie)
            }
          }
        } catch {
          case e: MissingVersion =>
            log.info("Couldn't find version " + e.version + " in log; resyncing")
            resync(datasetId, secondary, currentCookie, delogger)
          case _: ResyncException =>
            log.info("Incremental update requested full resync")
            resync(datasetId, secondary, currentCookie, delogger)
          case _: ResyncForPickySecondary =>
            log.info("Resyncing because secondary only wants published copies and we just got a publish event")
            resync(datasetId, secondary, currentCookie, delogger)
        }
      case None =>
        drop(secondary, datasetId, currentCookie)
    }
  }

  def drop(secondary: NamedSecondary[CT, CV], datasetId: DatasetId, cookie: Option[String]) {
    timingReport("drop", "dataset" -> datasetId) {
      secondary.store.dropDataset(datasetIdFormatter(datasetId), cookie)
      dropFromSecondaryMap(secondary, datasetId)
    }
  }

  def resync(datasetId: DatasetId, secondary: NamedSecondary[CT, CV], initialCookie: Option[String], delogger: Delogger[CV]) {
    timingReport("resync", "dataset" -> datasetId) {
      val w = new PostgresDatasetMapWriter(conn, typeNamespace, timingReport, () => sys.error("Secondary should not be generating obfuscation keys"), 0L)
      w.datasetInfo(datasetId, datasetLockTimeout) match {
        case Some(datasetInfo) =>
          val internalName = datasetIdFormatter(datasetId)
          val allCopies = w.allCopies(datasetInfo)
          var currentCookie = initialCookie
          val latest = w.latest(datasetInfo)
          val latestDataVersion = latest.dataVersion
          val latestLifecycleStage = latest.lifecycleStage
          for(copy <- allCopies) {
            timingReport("copy", "number" -> copy.copyNumber) {
              currentCookie =
                if(copy.lifecycleStage == LifecycleStage.Discarded) secondary.store.dropCopy(internalName, copy.copyNumber, currentCookie)
                else if(copy.lifecycleStage != LifecycleStage.Unpublished) syncCopy(secondary, internalName, new DatasetCopyContext(copy, w.schema(copy)), currentCookie)
                else if(secondary.store.wantsWorkingCopies) syncCopy(secondary, internalName, new DatasetCopyContext(copy, w.schema(copy)), currentCookie)
                else currentCookie
            }
          }
          updateSecondaryMap(secondary, datasetInfo.systemId, latestDataVersion, latestLifecycleStage, currentCookie)
        case None =>
          drop(secondary, datasetId, initialCookie)
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

  def syncCopy(secondary: NamedSecondary[CT, CV], internalName: String, copyCtx: DatasetCopyContext[CT], cookie: Secondary.Cookie): Secondary.Cookie =
    timingReport("sync-copy", "secondary" -> secondary.storeId, "dataset" -> copyCtx.datasetInfo.systemId, "copy" -> copyCtx.copyInfo.copyNumber) {
      secondary.store.resync(internalName, copyCtx, cookie, new SimpleArm[Iterator[Row[CV]]] {
        def flatMap[A](f: Iterator[Row[CV]] => A): A =
          new RepBasedDatasetExtractor(
            conn,
            copyCtx.copyInfo.dataTableName,
            repFor(copyCtx.systemIdCol_!).asPKableRep,
            copyCtx.schema.mapValuesStrict(repFor)).allRows(None, None).map(f)
      })
    }

  def updateSecondaryMap(secondary: NamedSecondary[CT, CV], datasetId: DatasetId, newLastDataVersion: Long, newLifecycleStage: LifecycleStage, newCookie: Secondary.Cookie) {
    secondaryManifest.completedReplicationTo(secondary.storeId, datasetId, newLastDataVersion, newLifecycleStage, newCookie)
    conn.commit()
  }

  def dropFromSecondaryMap(secondary: NamedSecondary[CT, CV], datasetId: DatasetId) {
    secondaryManifest.dropDataset(secondary.storeId, datasetId)
    conn.commit()
  }

  private class ResyncException extends ControlThrowable
  private class ResyncForPickySecondary extends ControlThrowable
}
