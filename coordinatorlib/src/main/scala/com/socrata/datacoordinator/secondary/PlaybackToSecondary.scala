package com.socrata.datacoordinator
package secondary

import scala.util.control.ControlThrowable
import scala.concurrent.duration.Duration

import com.rojoma.simplearm.util._
import org.slf4j.LoggerFactory

import com.socrata.datacoordinator.truth.loader.{Delogger, MissingVersion}
import com.socrata.datacoordinator.id.{RowId, DatasetId}
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.sql.SqlColumnReadRep
import com.socrata.datacoordinator.util.TimingReport
import com.socrata.datacoordinator.truth.universe._
import com.socrata.datacoordinator.truth.metadata
import com.socrata.soql.environment.TypeName

class PlaybackToSecondary[CT, CV](u: Universe[CT, CV] with Commitable with SecondaryManifestProvider with DatasetMapReaderProvider with DatasetMapWriterProvider with DatasetReaderProvider with DeloggerProvider, repFor: metadata.ColumnInfo[CT] => SqlColumnReadRep[CT, CV], typeForName: TypeName => Option[CT], datasetIdFormatter: DatasetId => String, timingReport: TimingReport) {
  val log = LoggerFactory.getLogger(classOf[PlaybackToSecondary[_,_]])

  val datasetLockTimeout = Duration.Inf

  class LifecycleStageTrackingIterator(underlying: Iterator[Delogger.LogEvent[CV]], initialStage: metadata.LifecycleStage) extends BufferedIterator[Delogger.LogEvent[CV]] {
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
        case Delogger.WorkingCopyCreated(_, _) =>
          metadata.LifecycleStage.Unpublished
        case Delogger.WorkingCopyDropped | Delogger.WorkingCopyPublished =>
          metadata.LifecycleStage.Published
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

  def makeSecondaryDatasetInfo(dsInfo: metadata.DatasetInfoLike) =
    DatasetInfo(datasetIdFormatter(dsInfo.systemId), dsInfo.localeName, dsInfo.obfuscationKey.clone())

  def makeSecondaryCopyInfo(copyInfo: metadata.CopyInfoLike) =
    CopyInfo(copyInfo.systemId, copyInfo.copyNumber, copyInfo.lifecycleStage.correspondingSecondaryStage, copyInfo.dataVersion)

  def makeSecondaryColumnInfo(colInfo: metadata.ColumnInfoLike) = {
    typeForName(TypeName(colInfo.typeName)) match {
      case Some(typ) =>
        ColumnInfo(colInfo.systemId, colInfo.userColumnId, typ, isSystemPrimaryKey = colInfo.isSystemPrimaryKey, isUserPrimaryKey = colInfo.isUserPrimaryKey, isVersion = colInfo.isVersion)
      case None =>
        sys.error("Typename " + colInfo.typeName + " got into the logs somehow!")
    }
  }

  private class UpdateOp(secondary: NamedSecondary[CT, CV],
                         job: SecondaryRecord)
  {
    private val datasetId = job.datasetId
    private var currentCookie = job.initialCookie
    private var currentLifecycleStage = job.startingLifecycleStage
    private val datasetMapReader = u.datasetMapReader

    def go() {
      datasetMapReader.datasetInfo(datasetId) match {
        case Some(datasetInfo) =>
          log.info("Found dataset " + datasetInfo.systemId + " in truth")
          try {
            for(dataVersion <- job.startingDataVersion to job.endingDataVersion) {
              playbackLog(datasetInfo, dataVersion)
            }
          } catch {
            case e: MissingVersion =>
              log.info("Couldn't find version {} in log; resyncing", e.version)
              resync()
            case ResyncSecondaryException(reason) =>
              log.info("Incremental update requested full resync: {}", reason)
              resync()
            case _: InternalResyncForPickySecondary =>
              log.info("Resyncing because secondary only wants published copies and we just got a publish event")
              resync()
          }
        case None =>
          drop()
      }
    }

    def convertOp(op: truth.loader.Operation[CV]): Operation[CV] = op match {
      case truth.loader.Insert(systemId, data) => Insert(systemId, data)
      case truth.loader.Update(systemId, oldData, newData) => Update(systemId, newData)(oldData)
      case truth.loader.Delete(systemId, oldData) => Delete(systemId)(oldData)
    }

    def convertEvent(ev: Delogger.LogEvent[CV]): Option[Event[CT, CV]] = ev match {
      case rdu: Delogger.RowDataUpdated[CV] =>
        Some(RowDataUpdated(rdu.operations.view.map(convertOp)))
      case Delogger.Truncated =>
        Some(Truncated)
      case Delogger.WorkingCopyDropped =>
        Some(WorkingCopyDropped)
      case Delogger.DataCopied =>
        Some(DataCopied)
      case Delogger.WorkingCopyPublished =>
        Some(WorkingCopyPublished)
      case Delogger.ColumnCreated(info) =>
        Some(ColumnCreated(makeSecondaryColumnInfo(info)))
      case Delogger.ColumnRemoved(info) =>
        Some(ColumnRemoved(makeSecondaryColumnInfo(info)))
      case Delogger.RowIdentifierSet(info) =>
        Some(RowIdentifierSet(makeSecondaryColumnInfo(info)))
      case Delogger.RowIdentifierCleared(info) =>
        Some(RowIdentifierCleared(makeSecondaryColumnInfo(info)))
      case Delogger.SystemRowIdentifierChanged(info) =>
        Some(SystemRowIdentifierChanged(makeSecondaryColumnInfo(info)))
      case Delogger.VersionColumnChanged(info) =>
        Some(VersionColumnChanged(makeSecondaryColumnInfo(info)))
      case Delogger.WorkingCopyCreated(datasetInfo, copyInfo) =>
        Some(WorkingCopyCreated(makeSecondaryCopyInfo(copyInfo)))
      case Delogger.SnapshotDropped(info) =>
        Some(SnapshotDropped(makeSecondaryCopyInfo(info)))
      case Delogger.CounterUpdated(nextCounter) =>
        None
      case Delogger.EndTransaction =>
        None
    }

    def playbackLog(datasetInfo: metadata.DatasetInfo, dataVersion: Long) {
      log.trace("Playing back version {}", dataVersion)
      val finalLifecycleStage = for {
        delogger <- managed(u.delogger(datasetInfo))
        rawIt <- managed(delogger.delog(dataVersion))
      } yield {
        val secondaryDatasetInfo = makeSecondaryDatasetInfo(datasetInfo)
        val it = new LifecycleStageTrackingIterator(rawIt, currentLifecycleStage)
        if(secondary.store.wantsWorkingCopies) {
          log.trace("Secondary store wants working copies; just blindly sending everything")
          currentCookie = secondary.store.version(secondaryDatasetInfo, dataVersion, currentCookie, it.flatMap(convertEvent))
        } else {
          while(it.hasNext) {
            if(currentLifecycleStage != metadata.LifecycleStage.Published) {
              log.trace("Current lifecycle stage in the secondary is {}; skipping data until I find a publish event", currentLifecycleStage)
              // skip until it IS published, then resync
              while(it.hasNext && it.stageAfterNextEvent != metadata.LifecycleStage.Published) it.next()
              if(it.hasNext) {
                log.trace("There is more.  Resyncing")
                throw new InternalResyncForPickySecondary
              } else {
                log.trace("There is no more.")
              }
              currentLifecycleStage = it.stageBeforeNextEvent
            } else {
              log.trace("Sending events for a published copy")
              val publishedIt = new StageLimitedIterator(it)
              if(publishedIt.hasNext) {
                log.trace("Sendsendsendsend")
                currentCookie = secondary.store.version(secondaryDatasetInfo, dataVersion, currentCookie, publishedIt.flatMap(convertEvent))
                publishedIt.finish()
                currentLifecycleStage = it.stageBeforeNextEvent
              } else {
                log.trace("First item must've been a copy-event")
                currentLifecycleStage = it.stageAfterNextEvent
              }
            }
          }
        }
        val res = it.finalLifecycleStage()
        log.trace("Final lifecycle stage is {}", res)
        res
      }
      updateSecondaryMap(dataVersion, finalLifecycleStage)
      currentLifecycleStage = finalLifecycleStage
    }

    def drop() {
      timingReport("drop", "dataset" -> datasetId) {
        secondary.store.dropDataset(datasetIdFormatter(datasetId), currentCookie)
        dropFromSecondaryMap()
      }
    }

    def resync() {
      while(true) {
        try {
          timingReport("resync", "dataset" -> datasetId) {
            val w = u.datasetMapWriter
            w.datasetInfo(datasetId, datasetLockTimeout) match {
              case Some(datasetInfo) =>
                val allCopies = w.allCopies(datasetInfo)
                val latest = w.latest(datasetInfo)
                val latestDataVersion = latest.dataVersion
                val latestLifecycleStage = latest.lifecycleStage
                for(copy <- allCopies) {
                  val secondaryDatasetInfo = makeSecondaryDatasetInfo(copy.datasetInfo)
                  timingReport("copy", "number" -> copy.copyNumber) {
                    if(copy.lifecycleStage == metadata.LifecycleStage.Discarded) currentCookie = secondary.store.dropCopy(secondaryDatasetInfo.internalName, copy.copyNumber, currentCookie)
                    else if(copy.lifecycleStage != metadata.LifecycleStage.Unpublished) syncCopy(copy)
                    else if(secondary.store.wantsWorkingCopies) syncCopy(copy)
                    else { /* ok */ }
                  }
                }
                updateSecondaryMap(latestDataVersion, latestLifecycleStage)
              case None =>
                drop()
            }
          }
          return
        } catch {
          case ResyncSecondaryException(reason) =>
            log.warn("Received resync while resyncing.  Resyncing as requested after waiting 10 seconds.  Reason: " + reason)
            Thread.sleep(10L * 1000);
        }
      }
    }

    def syncCopy(copyInfo: metadata.CopyInfo) {
      timingReport("sync-copy", "secondary" -> secondary.storeId, "dataset" -> copyInfo.datasetInfo.systemId, "copy" -> copyInfo.copyNumber) {
        for(reader <- u.datasetReader.openDataset(copyInfo)) {
          val copyCtx = new DatasetCopyContext(reader.copyInfo, reader.schema)
          val secondaryDatasetInfo = makeSecondaryDatasetInfo(copyCtx.datasetInfo)
          val secondaryCopyInfo = makeSecondaryCopyInfo(copyCtx.copyInfo)
          val secondarySchema = copyCtx.schema.mapValuesStrict(makeSecondaryColumnInfo)
          currentCookie = secondary.store.resync(secondaryDatasetInfo, secondaryCopyInfo, secondarySchema, currentCookie, reader.rows())
        }
      }
    }

    def updateSecondaryMap(newLastDataVersion: Long, newLifecycleStage: metadata.LifecycleStage) {
      u.secondaryManifest.completedReplicationTo(secondary.storeId, datasetId, newLastDataVersion, newLifecycleStage, currentCookie)
      u.commit()
    }

    def dropFromSecondaryMap() {
      u.secondaryManifest.dropDataset(secondary.storeId, datasetId)
      u.commit()
    }

    private class InternalResyncForPickySecondary extends ControlThrowable
  }
}
