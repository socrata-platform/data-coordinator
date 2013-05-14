package com.socrata.datacoordinator
package secondary

import scala.util.control.ControlThrowable
import scala.concurrent.duration.Duration

import com.rojoma.simplearm.util._
import org.slf4j.LoggerFactory

import com.socrata.datacoordinator.truth.loader.{MissingVersion, Delogger}
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.sql.SqlColumnReadRep
import com.socrata.datacoordinator.util.TimingReport
import com.socrata.datacoordinator.truth.loader.Delogger.{WorkingCopyPublished, WorkingCopyDropped, WorkingCopyCreated}
import com.socrata.datacoordinator.truth.universe._
import com.socrata.datacoordinator.truth.metadata.ColumnInfo

class PlaybackToSecondary[CT, CV](u: Universe[CT, CV] with Commitable with SecondaryManifestProvider with DatasetMapReaderProvider with DatasetMapWriterProvider with DatasetReaderProvider with DeloggerProvider, repFor: ColumnInfo[CT] => SqlColumnReadRep[CT, CV], datasetIdFormatter: DatasetId => String, timingReport: TimingReport) {
  val log = LoggerFactory.getLogger(classOf[PlaybackToSecondary[_,_]])

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

  def apply(secondary: NamedSecondary[CT, CV], job: SecondaryRecord) {
    new UpdateOp(secondary, job).go()
  }

  def drop(secondary: NamedSecondary[CT, CV], job: SecondaryRecord) {
    new UpdateOp(secondary, job).drop()
  }

  private class UpdateOp(secondary: NamedSecondary[CT, CV],
                         job: SecondaryRecord)
  {
    private val datasetId = job.datasetId
    private val internalName = datasetIdFormatter(datasetId)
    private var currentCookie = job.initialCookie
    private var currentLifecycleStage = job.startingLifecycleStage
    private var currentSecondaryVersion = secondary.store.currentVersion(internalName, currentCookie)
    private val datasetMapReader = u.datasetMapReader

    def go() {
      datasetMapReader.datasetInfo(datasetId) match {
        case Some(datasetInfo) =>
          log.info("Found dataset " + datasetInfo.systemId + " in truth")
          try {
            for(dataVersion <- job.startingDataVersion to job.endingDataVersion) {
              if(currentSecondaryVersion < dataVersion - 1) throw new ResyncForMissedVersion
              else if(currentSecondaryVersion == dataVersion - 1) {
                // right on schedule...
                playbackLog(datasetInfo, dataVersion)
              } else if(dataVersion > job.endingDataVersion) {
                // So far ahead even truth doesn't know about it?  That's impossible!
                throw new ResyncForBeingTooFarAhead
              } else {
                // it's ahead, but not so far ahead that it's completely implausible
                skipLog(datasetInfo, dataVersion)
              }
            }
          } catch {
            case e: MissingVersion =>
              log.info("Couldn't find version " + e.version + " in log; resyncing")
              resync()
            case e: ResyncForMissedVersion =>
              log.info("Secondary reported having a version earlier than what I thought it had; resyncing")
              resync()
            case _: ResyncException =>
              log.info("Incremental update requested full resync")
              resync()
            case _: ResyncForPickySecondary =>
              log.info("Resyncing because secondary only wants published copies and we just got a publish event")
              resync()
            case _: ResyncForBeingTooFarAhead =>
              log.warn("Resyncing because the secondary is farther ahead than truth!")
              resync()
          }
        case None =>
          drop()
      }
    }

    def playbackLog(datasetInfo: DatasetInfo, dataVersion: Long) {
      val finalLifecycleStage = for {
        delogger <- managed(u.delogger(datasetInfo))
        rawIt <- managed(delogger.delog(dataVersion))
      } yield {
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
        it.finalLifecycleStage()
      }
      updateSecondaryMap(dataVersion, finalLifecycleStage)
      currentLifecycleStage = finalLifecycleStage
      currentSecondaryVersion = dataVersion
    }

    def skipLog(datasetInfo: DatasetInfo, dataVersion: Long) {
      // hmph, since I need to track lifecycle stage changes I still need to run through the iterator...
      val finalLifecycleStage = for {
        delogger <- managed(u.delogger(datasetInfo))
        rawIt <- managed(delogger.delog(dataVersion))
      } yield {
        val it = new LifecycleStageTrackingIterator(rawIt, currentLifecycleStage)
        it.finalLifecycleStage()
      }
      updateSecondaryMap(dataVersion, finalLifecycleStage)
      currentLifecycleStage = finalLifecycleStage
      currentSecondaryVersion = dataVersion
    }

    def drop() {
      timingReport("drop", "dataset" -> datasetId) {
        secondary.store.dropDataset(datasetIdFormatter(datasetId), currentCookie)
        dropFromSecondaryMap()
      }
    }

    def resync() {
      timingReport("resync", "dataset" -> datasetId) {
        val w = u.datasetMapWriter
        w.datasetInfo(datasetId, datasetLockTimeout) match {
          case Some(datasetInfo) =>
            val allCopies = w.allCopies(datasetInfo)
            val latest = w.latest(datasetInfo)
            val latestDataVersion = latest.dataVersion
            val latestLifecycleStage = latest.lifecycleStage
            for(copy <- allCopies) {
              timingReport("copy", "number" -> copy.copyNumber) {
                if(copy.lifecycleStage == LifecycleStage.Discarded) currentCookie = secondary.store.dropCopy(internalName, copy.copyNumber, currentCookie)
                else if(copy.lifecycleStage != LifecycleStage.Unpublished) syncCopy(copy)
                else if(secondary.store.wantsWorkingCopies) syncCopy(copy)
                else { /* ok */ }
              }
            }
            updateSecondaryMap(latestDataVersion, latestLifecycleStage)
          case None =>
            drop()
        }
      }
    }

    def syncCopy(copyInfo: CopyInfo) {
      timingReport("sync-copy", "secondary" -> secondary.storeId, "dataset" -> copyInfo.datasetInfo.systemId, "copy" -> copyInfo.copyNumber) {
        for(reader <- u.datasetReader.openDataset(copyInfo)) {
          val copyCtx = new DatasetCopyContext(reader.copyInfo, reader.schema)
          currentCookie = secondary.store.resync(internalName, copyCtx, currentCookie, reader.rows())
        }
      }
    }

    def updateSecondaryMap(newLastDataVersion: Long, newLifecycleStage: LifecycleStage) {
      u.secondaryManifest.completedReplicationTo(secondary.storeId, datasetId, newLastDataVersion, newLifecycleStage, currentCookie)
      u.commit()
    }

    def dropFromSecondaryMap() {
      u.secondaryManifest.dropDataset(secondary.storeId, datasetId)
      u.commit()
    }

    private class ResyncException extends ControlThrowable
    private class ResyncForPickySecondary extends ControlThrowable
    private class ResyncForMissedVersion extends ControlThrowable
    private class ResyncForBeingTooFarAhead extends ControlThrowable
  }
}
