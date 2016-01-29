package com.socrata.datacoordinator
package secondary

import com.rojoma.simplearm.util._
import com.rojoma.simplearm.SimpleArm
import com.socrata.datacoordinator.id.{RowId, DatasetId}
import com.socrata.datacoordinator.truth.loader.{Delogger, MissingVersion}
import com.socrata.datacoordinator.truth.metadata
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.sql.SqlColumnReadRep
import com.socrata.datacoordinator.truth.universe._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.util.TimingReport
import com.socrata.soql.environment.TypeName
import com.socrata.thirdparty.metrics.Metrics
import com.typesafe.scalalogging.slf4j.Logging
import scala.concurrent.duration.Duration
import scala.util.control.ControlThrowable

object PlaybackToSecondary {
  type SuperUniverse[CT, CV] = Universe[CT, CV] with Commitable with
                                                     SecondaryManifestProvider with
                                                     DatasetMapReaderProvider with
                                                     DatasetMapWriterProvider with
                                                     DatasetReaderProvider with
                                                     DeloggerProvider
}

class PlaybackToSecondary[CT, CV](u: PlaybackToSecondary.SuperUniverse[CT, CV],
                                  repFor: metadata.ColumnInfo[CT] => SqlColumnReadRep[CT, CV],
                                  typeForName: TypeName => Option[CT],
                                  datasetIdFormatter: DatasetId => String,
                                  timingReport: TimingReport) extends Logging {
  val datasetLockTimeout = Duration.Inf

  class LifecycleStageTrackingIterator(underlying: Iterator[Delogger.LogEvent[CV]],
                                       initialStage: metadata.LifecycleStage)
      extends BufferedIterator[Delogger.LogEvent[CV]] {
    private var currentStage = initialStage
    private var lookahead: Delogger.LogEvent[CV] = null

    def stageBeforeNextEvent: metadata.LifecycleStage = currentStage

    def stageAfterNextEvent: metadata.LifecycleStage = computeNextStage(head)

    def hasNext: Boolean = lookahead != null || underlying.hasNext

    def head: Delogger.LogEvent[CV] = {
      if(lookahead == null) lookahead = advance()
      lookahead
    }

    private def advance() = underlying.next()

    def next(): Delogger.LogEvent[CV] = {
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

    def finalLifecycleStage(): metadata.LifecycleStage = {
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

    def hasNext: Boolean = underlying.hasNext && underlying.stageAfterNextEvent == wantedStage
    def next(): Delogger.LogEvent[CV] =
      if(hasNext) underlying.next()
      else Iterator.empty.next()

    def finish(): Unit = while(hasNext) next()
  }

  // Instruments via metrics and logging the iteration of rows/events
  class InstrumentedIterator[T](name: String,
                                datasetName: String,
                                underlying: Iterator[T],
                                loggingRate: Int = 10000) extends Iterator[T] with Metrics {
    var itemNum = 0
    val meter = metrics.meter(name, "rows")

    def hasNext: Boolean = underlying.hasNext
    def next(): T = {
      meter.mark()
      itemNum += 1
      if(itemNum % loggingRate == 0)
        logger.info("[{}] {}: {} rows/events processed", name, datasetName, itemNum.toString)
      underlying.next()
    }
  }

  def apply(secondary: NamedSecondary[CT, CV], job: SecondaryRecord): Unit = {
    // Normally, UpdateOp will drop the dataset if it is not in truth.
    // This check allows us to just drop a dataset in a specific secondary by
    // running a SQL like:
    //   UPDATE secondary_manifest set latest_secondary_lifecycle_stage = 'Discarded', latest_secondary_data_version = -1
    //    WHERE dataset_system_id = $ID
    // TODO: Add an endpoint to SODA Fountain.  Plumb it through Data Coordinator to do that.
    if(job.startingLifecycleStage == metadata.LifecycleStage.Discarded && job.startingDataVersion <= 0)
      new UpdateOp(secondary, job).drop()
    else
      new UpdateOp(secondary, job).go()
  }

  def drop(secondary: NamedSecondary[CT, CV], job: SecondaryRecord): Unit = {
    new UpdateOp(secondary, job).drop()
  }

  def makeSecondaryDatasetInfo(dsInfo: metadata.DatasetInfoLike) =
    DatasetInfo(datasetIdFormatter(dsInfo.systemId), dsInfo.localeName, dsInfo.obfuscationKey.clone())

  def makeSecondaryCopyInfo(copyInfo: metadata.CopyInfoLike) =
    CopyInfo(copyInfo.systemId,
             copyInfo.copyNumber,
             copyInfo.lifecycleStage.correspondingSecondaryStage,
             copyInfo.dataVersion,
             copyInfo.lastModified)

  def makeSecondaryRollupInfo(rollupInfo: metadata.RollupInfoLike) =
    RollupInfo(rollupInfo.name.underlying, rollupInfo.soql)

  def makeSecondaryColumnInfo(colInfo: metadata.ColumnInfoLike) = {
    typeForName(TypeName(colInfo.typeName)) match {
      case Some(typ) =>
        ColumnInfo(colInfo.systemId,
                   colInfo.userColumnId,
                   colInfo.fieldName,
                   typ,
                   isSystemPrimaryKey = colInfo.isSystemPrimaryKey,
                   isUserPrimaryKey = colInfo.isUserPrimaryKey,
                   isVersion = colInfo.isVersion,
                   colInfo.computationStrategyInfo.map { strategy =>
                     ComputationStrategyInfo(strategy.strategyType, strategy.sourceColumnIds, strategy.parameters)
                   })
      case None =>
        sys.error("Typename " + colInfo.typeName + " got into the logs somehow!")
    }
  }



  private class UpdateOp(secondary: NamedSecondary[CT, CV],
                         job: SecondaryRecord)
  {
    private val datasetId = job.datasetId
    private val claimantId = job.claimantId
    private var currentCookie = job.initialCookie
    private var currentLifecycleStage = job.startingLifecycleStage
    private val datasetMapReader = u.datasetMapReader

    def go(): Unit = {
      datasetMapReader.datasetInfo(datasetId) match {
        case Some(datasetInfo) =>
          logger.info("Found dataset " + datasetInfo.systemId + " in truth")
          try {
            for(dataVersion <- job.startingDataVersion to job.endingDataVersion) {
              playbackLog(datasetInfo, dataVersion)
            }
          } catch {
            case e: MissingVersion =>
              logger.info("Couldn't find version {} in log; resyncing", e.version.toString)
              resync()
            case ResyncSecondaryException(reason) =>
              logger.info("Incremental update requested full resync: {}", reason)
              resync()
            case _: InternalResyncForPickySecondary =>
              logger.info("Resyncing because secondary only wants published copies and we just got a publish event")
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
      case Delogger.FieldNameUpdated(info) =>
        Some(FieldNameUpdated(makeSecondaryColumnInfo(info)))
      case Delogger.RowIdentifierSet(info) =>
        Some(RowIdentifierSet(makeSecondaryColumnInfo(info)))
      case Delogger.RowIdentifierCleared(info) =>
        Some(RowIdentifierCleared(makeSecondaryColumnInfo(info)))
      case Delogger.SystemRowIdentifierChanged(info) =>
        Some(SystemRowIdentifierChanged(makeSecondaryColumnInfo(info)))
      case Delogger.VersionColumnChanged(info) =>
        Some(VersionColumnChanged(makeSecondaryColumnInfo(info)))
      case Delogger.LastModifiedChanged(lastModified) =>
        Some(LastModifiedChanged(lastModified))
      case Delogger.WorkingCopyCreated(datasetInfo, copyInfo) =>
        Some(WorkingCopyCreated(makeSecondaryCopyInfo(copyInfo)))
      case Delogger.SnapshotDropped(info) =>
        Some(SnapshotDropped(makeSecondaryCopyInfo(info)))
      case Delogger.CounterUpdated(nextCounter) =>
        None
      case Delogger.RollupCreatedOrUpdated(info) =>
        Some(RollupCreatedOrUpdated(makeSecondaryRollupInfo(info)))
      case Delogger.RollupDropped(info) =>
        Some(RollupDropped(makeSecondaryRollupInfo(info)))
      case Delogger.EndTransaction =>
        None
    }

    private def setLifecycleStage(newStage: metadata.LifecycleStage, info: metadata.DatasetInfo): Unit = {
      logger.info("{}: New lifecycle stage: {}", info.systemId.toString, newStage)
      currentLifecycleStage = newStage
    }

    def playbackLog(datasetInfo: metadata.DatasetInfo, dataVersion: Long): Unit = {
      logger.trace("Playing back version {}", dataVersion.toString)
      val finalLifecycleStage = for {
        delogger <- managed(u.delogger(datasetInfo))
        rawIt <- managed(delogger.delog(dataVersion))
      } yield {
        val secondaryDatasetInfo = makeSecondaryDatasetInfo(datasetInfo)
        val instrumentedIt = new InstrumentedIterator("playback-log-throughput",
                                                      datasetInfo.systemId.toString,
                                                      rawIt)
        val it = new LifecycleStageTrackingIterator(instrumentedIt, currentLifecycleStage)
        if(secondary.store.wantsWorkingCopies) {
          logger.trace("Secondary store wants working copies; just blindly sending everything")
          currentCookie = secondary.store.version(secondaryDatasetInfo, dataVersion,
                                                  currentCookie, it.flatMap(convertEvent))
        } else {
          while(it.hasNext) {
            if(currentLifecycleStage != metadata.LifecycleStage.Published) {
              logger.trace("Current lifecycle stage in the secondary is {}; " +
                        "skipping data until I find a publish event", currentLifecycleStage)
              // skip until it IS published, then resync
              while(it.hasNext && it.stageAfterNextEvent != metadata.LifecycleStage.Published) it.next()
              if(it.hasNext) {
                logger.trace("There is more.  Resyncing")
                throw new InternalResyncForPickySecondary
              } else {
                logger.trace("There is no more.")
              }
              setLifecycleStage(it.stageBeforeNextEvent, datasetInfo)
            } else {
              logger.trace("Sending events for a published copy")
              val publishedIt = new StageLimitedIterator(it)
              if(publishedIt.hasNext) {
                logger.trace("Sendsendsendsend")
                currentCookie = secondary.store.version(secondaryDatasetInfo,
                                                        dataVersion,
                                                        currentCookie,
                                                        publishedIt.flatMap(convertEvent))
                publishedIt.finish()
                setLifecycleStage(it.stageBeforeNextEvent, datasetInfo)
              } else {
                logger.trace("First item must've been a copy-event")
                setLifecycleStage(it.stageAfterNextEvent, datasetInfo)
              }
            }
          }
        }
        val res = it.finalLifecycleStage()
        logger.trace("Final lifecycle stage is {}", res)
        res
      }
      updateSecondaryMap(dataVersion, finalLifecycleStage)
      setLifecycleStage(finalLifecycleStage, datasetInfo)
    }

    def drop(): Unit = {
      timingReport("drop", "dataset" -> datasetId) {
        secondary.store.dropDataset(datasetIdFormatter(datasetId), currentCookie)
        dropFromSecondaryMap()
      }
    }

    def resync(): Unit = {
      while(true) {
        try {
          timingReport("resync", "dataset" -> datasetId) {
            val w = u.datasetMapWriter
            w.datasetInfo(datasetId, datasetLockTimeout) match {
              case Some(datasetInfo) =>
                val allCopies = w.allCopies(datasetInfo) // guarantied to be ordered by copy number
                val latest = w.latest(datasetInfo)
                val latestDataVersion = latest.dataVersion
                val latestLifecycleStage = latest.lifecycleStage
                for(copy <- allCopies) {
                  val secondaryDatasetInfo = makeSecondaryDatasetInfo(copy.datasetInfo)
                  timingReport("copy", "number" -> copy.copyNumber) {
                    // secondary.store.resync(.) will be called
                    // on copies in order by their copy number
                    val isLatestCopy = copy.copyNumber == latest.copyNumber
                    if(copy.lifecycleStage == metadata.LifecycleStage.Discarded) {
                      currentCookie = secondary.store.dropCopy(secondaryDatasetInfo.internalName,
                                                               copy.copyNumber,
                                                               currentCookie)
                    } else if(copy.lifecycleStage != metadata.LifecycleStage.Unpublished) {
                      syncCopy(copy, isLatestCopy)
                    } else if(secondary.store.wantsWorkingCopies) {
                      syncCopy(copy, isLatestCopy)
                    } else { /* ok */ }
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
            logger.warn("Received resync while resyncing.  Resyncing as requested after waiting 10 seconds. " +
                     " Reason: " + reason)
            Thread.sleep(10L * 1000)
        }
      }
    }

    def syncCopy(copyInfo: metadata.CopyInfo, isLatestCopy: Boolean): Unit = {
      timingReport("sync-copy",
                   "secondary" -> secondary.storeId,
                   "dataset" -> copyInfo.datasetInfo.systemId,
                   "copy" -> copyInfo.copyNumber) {
        for(reader <- u.datasetReader.openDataset(copyInfo)) {
          val copyCtx = new DatasetCopyContext(reader.copyInfo, reader.schema)
          val secondaryDatasetInfo = makeSecondaryDatasetInfo(copyCtx.datasetInfo)
          val secondaryCopyInfo = makeSecondaryCopyInfo(copyCtx.copyInfo)
          val secondarySchema = copyCtx.schema.mapValuesStrict(makeSecondaryColumnInfo)
          val itRows = reader.rows(sorted=false)
          // Sigh. itRows is a simple-arm v1 Managed.  v2 has a monad map() which makes the code below
          // much, much shorter.
          val wrappedRows = new SimpleArm[Iterator[ColumnIdMap[CV]]] {
            def flatMap[A](f: Iterator[ColumnIdMap[CV]] => A): A = {
              itRows.flatMap { it: Iterator[ColumnIdMap[CV]] =>
                f(new InstrumentedIterator("sync-copy-throughput",
                                           copyInfo.datasetInfo.systemId.toString,
                                           it))
              }
            }
          }
          val rollups: Seq[RollupInfo] = u.datasetMapReader.rollups(copyInfo).toSeq.
                                           map(makeSecondaryRollupInfo)
          currentCookie = secondary.store.resync(secondaryDatasetInfo,
                                                 secondaryCopyInfo,
                                                 secondarySchema,
                                                 currentCookie,
                                                 wrappedRows,
                                                 rollups,
                                                 isLatestCopy)
        }
      }
    }

    def updateSecondaryMap(newLastDataVersion: Long, newLifecycleStage: metadata.LifecycleStage): Unit = {
      u.secondaryManifest.completedReplicationTo(secondary.storeId,
                                                 claimantId,
                                                 datasetId,
                                                 newLastDataVersion,
                                                 newLifecycleStage,
                                                 currentCookie)
      u.commit()
    }

    def dropFromSecondaryMap(): Unit = {
      u.secondaryManifest.dropDataset(secondary.storeId, datasetId)
      u.commit()
    }

    private class InternalResyncForPickySecondary extends ControlThrowable
  }
}
