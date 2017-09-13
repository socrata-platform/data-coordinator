package com.socrata.datacoordinator.secondary.feedback

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.interpolation._
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.secondary
import com.socrata.datacoordinator.secondary.feedback.monitor.StatusMonitor
import com.socrata.datacoordinator.secondary.{BrokenDatasetSecondaryException, ReplayLaterSecondaryException}
import com.socrata.datacoordinator.util.collection.MutableColumnIdMap

case class Row[CV](data: secondary.Row[CV], oldData: Option[secondary.Row[CV]])

sealed abstract class FeedbackResult

case class Success(feedbackCookie: FeedbackCookie) extends FeedbackResult
case class ReplayLater(reason: String, feedbackCookie: FeedbackCookie => FeedbackCookie) extends FeedbackResult
case class BrokenDataset(reason: String, feedbackCookie: FeedbackCookie => FeedbackCookie) extends FeedbackResult
case class FeedbackError(reason: String, cause: Throwable) extends FeedbackResult

object FeedbackContext {
  def apply[CT,CV](user: String,
                   batchSize: Int => Int,
                   statusMonitor: StatusMonitor,
                   computationHandlers:  Seq[ComputationHandler[CT,CV]],
                   computationRetryLimit: Int,
                   dataCoordinator: (String, CT => JValue => Option[CV]) => DataCoordinatorClient[CT,CV],
                   dataCoordinatorRetryLimit: Int,
                   datasetContext: (String, CV => JValue, CT => JValue => Option[CV])): FeedbackCookie => FeedbackContext[CT,CV] = {
    val (datasetInternalName, toJValueFunc, fromJValueFunc) = datasetContext
    new FeedbackContext(
      user,
      batchSize,
      statusMonitor,
      computationHandlers,
      computationRetryLimit,
      dataCoordinator,
      dataCoordinatorRetryLimit,
      datasetInternalName,
      toJValueFunc,
      fromJValueFunc,
      _
    )
  }
}

class FeedbackContext[CT,CV](user: String,
                             batchSize: Int => Int,
                             statusMonitor: StatusMonitor,
                             computationHandlers: Seq[ComputationHandler[CT,CV]],
                             computationRetryLimit: Int,
                             dataCoordinator: (String, CT => JValue => Option[CV]) => DataCoordinatorClient[CT,CV],
                             dataCoordinatorRetryLimit: Int,
                             datasetInternalName: String,
                             toJValueFunc: CV => JValue,
                             fromJValueFunc: CT => JValue => Option[CV],
                             currentCookie: FeedbackCookie) {

  private def success(current: CookieSchema): Success = {
    Success(currentCookie.copyCurrent(
      current = current,
      computationRetriesLeft = computationRetryLimit,
      dataCoordinatorRetriesLeft = dataCoordinatorRetryLimit,
      resync = false,
      errorMessage = None
    ))
  }

  private def replayLater(reason: String, resync: Boolean): ReplayLater = {
    ReplayLater(reason, _.copyCurrent(resync = resync, errorMessage = Some(reason)))
  }

  private def replayComputation(reason: String, resync: Boolean): ReplayLater = {
    ReplayLater(reason, { feedbackCookie =>
      feedbackCookie.copyCurrent(
        computationRetriesLeft = feedbackCookie.current.computationRetriesLeft - 1,
        dataCoordinatorRetriesLeft = dataCoordinatorRetryLimit,
        resync = resync,
        errorMessage = Some(reason))
    })
  }

  private def replayDataCoordinator(reason: String, resync: Boolean): ReplayLater = {
    ReplayLater(reason, { feedbackCookie =>
      feedbackCookie.copyCurrent(
        computationRetriesLeft = computationRetryLimit,
        dataCoordinatorRetriesLeft = feedbackCookie.current.dataCoordinatorRetriesLeft - 1,
        resync = resync,
        errorMessage = Some(reason))
    })
  }

  private def brokenDataset(reason: String, resync: Boolean): BrokenDataset = {
    BrokenDataset(reason, _.copyCurrent(resync = resync, errorMessage = Some(reason)))
  }

  val log = org.slf4j.LoggerFactory.getLogger(classOf[FeedbackContext[CT,CV]])
  private var cookie = currentCookie.current

  val dataCoordinatorClient = dataCoordinator(datasetInternalName, fromJValueFunc)

  def mergeWith[A, B](xs: Map[A, B], ys: Map[A, B])(f: (B, B) => B): Map[A, B] = {
    ys.foldLeft(xs) { (combined, yab) =>
      val (a, yb) = yab
      val newB = combined.get(a) match {
        case None => yb
        case Some(xb) => f(xb, yb)
      }
      combined.updated(a, newB)
    }
  }

  private def computeUpdates(computationHandler: ComputationHandler[CT,CV],
                             rows: IndexedSeq[Row[CV]],
                             targetColumns: Set[UserColumnId]): Either[ComputationFailure, Map[Int, Map[UserColumnId, CV]]] = {
    val perDatasetData = computationHandler.setupDataset(cookie)
    val perColumnData = cookie.strategyMap.toSeq.collect {
      case (targetCol, strat) if targetColumns.contains(targetCol) & computationHandler.matchesStrategyType(strat.strategyType) =>
        computationHandler.setupColumn(perDatasetData, strat, targetCol)
    }
    val toCompute = rows.iterator.zipWithIndex.flatMap { case (row, index) =>
      val rcis =
        perColumnData.flatMap { columnData =>
          // don't compute if there has been to change to the source columns
          if (noChange(row, columnData.strategy.sourceColumnIds))
            None
          else
            Some(computationHandler.setupCell(columnData, row.data))
        }
      if (rcis.isEmpty) Iterator.empty
      else Iterator.single(index -> rcis)
    }.toMap

    computationHandler.compute(toCompute) match {
      case Right(results) =>
        Right(results.flatMap { case (rowIdx, updates) =>
          val row = rows(rowIdx)
          val filtered = updates.filter { case (colId: UserColumnId, value) =>
            extractCV(row.data, colId) != Some(value) // don't update to current value
          }
          val rowId = extractCV(row.data, cookie.primaryKey) match {
            case None => throw new Exception(s"Cannot find value for primary key ${cookie.primaryKey} in row: $row!") // throwing as exception because this should not happen _ever_
            case Some(other) => other
          }
          if (filtered.nonEmpty) {
            val newRow = Map(
              cookie.primaryKey -> rowId
            ) ++ filtered.map { case (id, value) => (id, value)}.toMap
            Some(rowIdx -> newRow)
          } else {
            None
          }
        })
      case failure => failure
    }
  }

  private def noChange(row: Row[CV], columns: Seq[UserColumnId]): Boolean = row.oldData match {
    case Some(old) =>
      columns.forall { id =>
        val internal = cookie.columnIdMap(id)
        row.data(internal) == old(internal) // safe because updates contain _all_ row values (and this must be one)
      }
    case None => false
  }

  private def extractCV(row: secondary.Row[CV], userId: UserColumnId): Option[CV] = {
    val internal = cookie.columnIdMap(userId)
    row.get(internal)
  }

  // commands for mutation scripts
  private val commands: Seq[JValue] =
    j"""[ { "c" : "normal", "user" : $user }
        , { "c" : "row data", "update_only" : true, "nonfatal_row_errors" : [ "insert_in_update_only", "no_such_row_to_update" ] }
        ]""".toSeq

  private def writeMutationScript(rowUpdates: Iterator[JValue]): Option[JArray] = {
    if (!rowUpdates.hasNext) None
    else Some(JArray(commands ++ rowUpdates))
  }

  // this may throw a ReplayLaterSecondaryException or a BrokenDatasetSecondaryException
  def feedback(rows: Iterator[Row[CV]],
               targetColumns: Set[UserColumnId] = cookie.strategyMap.keySet,
               resync: Boolean = false): FeedbackResult = {
    val width = cookie.columnIdMap.size
    val size = batchSize(width)

    log.info("Feeding back rows with batch_size = {} for target computed columns: {}", size, targetColumns)

    var count = 0
    rows.grouped(size).foreach { batchSeq =>
      val batch: Seq[Row[CV]] = batchSeq.toIndexedSeq
      count += 1
      val updates = computationHandlers.foldLeft(Map.empty[Int, Map[UserColumnId, CV]]) { (currentUpdates, computationHandler) =>
        val currentRows = batch.toArray
        for ((idx, upds) <- currentUpdates) {
          val currentRow = MutableColumnIdMap(currentRows(idx).data)
          for ((cid, cv) <- upds) {
            currentRow(cookie.columnIdMap(cid)) = cv
          }
          currentRows(idx) = currentRows(idx).copy(data = currentRow.freeze())
        }
        computeUpdates(computationHandler, currentRows, targetColumns) match {
          case Right(newUpdates) =>
            mergeWith(currentUpdates, newUpdates)(_ ++ _)
          case Left(ComputationError(reason, cause)) =>
            // Some failure has occurred in computation; we will only retry these exceptions so many times
            return replayComputation(reason, resync)
          case Left(FatalComputationError(reason, cause)) =>
            // Some fatal failure has occurred in computation; the dataset should be marked broken
            return brokenDataset(reason, resync)
        }
      }
      val jsonUpdates = updates.valuesIterator.map { updates =>
        JsonEncode.toJValue(updates.mapValues(toJValueFunc))
      }
      writeMutationScript(jsonUpdates) match {
        case Some(script) =>
          dataCoordinatorClient.postMutationScript(script, cookie) match {
            case None =>
              log.info("Finished batch {} of approx. {} rows", count, size)
              statusMonitor.update(datasetInternalName, cookie.dataVersion, size, count)
            case Some(Right(TargetColumnDoesNotExist(column))) =>
              // this is pretty lame; but better than doing a full resync
              val deleted = deleteColumns(Set(column))
              computeColumns(targetColumns -- deleted)
            case Some(Right(PrimaryKeyColumnHasChanged)) =>
              // the primary key column has changed; try again
              // computeColumns(.) will discover the new primary key
              computeColumns(targetColumns)
            case Some(Right(PrimaryKeyColumnDoesNotExist(column))) =>
              // that's okay, we can try again
              val deleted = deleteColumns(Set(column))
              computeColumns(targetColumns -- deleted)
            case Some(Left(ftddc@FailedToDiscoverDataCoordinator)) =>
              log.warn("Failed to discover data-coordinator; going to request to have this dataset replayed later")
              // we will retry this "indefinitely"; do not decrement retries left
              return replayLater(ftddc.english, resync)
            case Some(Left(ddb@DataCoordinatorBusy)) =>
              log.info("Received a 409 from data-coordinator; going to request to have this dataset replayed later")
              // we will retry this "indefinitely"; do not decrement retries left
              return replayLater(ddb.english, resync)
            case Some(Left(ddne@DatasetDoesNotExist)) =>
              log.info("Completed updating dataset {} to version {} early: {}", datasetInternalName, cookie.dataVersion.underlying.toString, ddne.english)
              statusMonitor.remove(datasetInternalName, cookie.dataVersion, count)
              // nothing more to do here
              return success(cookie)
            case Some(Left(UnexpectedError(reason, cause))) =>
              // We failed to post our mutation script to data-coordinator for some reason
              return replayDataCoordinator(reason, resync) // TODO: use cause
          }
        case None =>
          log.info("Batch {} had no rows to update of approx. {} rows; no script posted.", count, size)
          statusMonitor.update(datasetInternalName, cookie.dataVersion, size, count)
      }
    }
    log.info("Completed row update of dataset {} in version {} after batch: {}",
      datasetInternalName, cookie.dataVersion.underlying.toString, count.toString)
    statusMonitor.remove(datasetInternalName, cookie.dataVersion, count)

    // success!
    success(cookie)
  }

  def flushColumnCreations(newColumns: Set[UserColumnId]): FeedbackResult = {
    if (newColumns.nonEmpty) {
      log.info("Flushing newly created columns...")
      val result = computeColumns(newColumns)
      log.info("Done flushing columns")
      result
    } else {
      success(cookie)
    }
  }

  // this is "safe" because we must be caught up before a publication stage change can be made
  private def deleteColumns(columns: Set[UserColumnId]): Set[UserColumnId] = {
    log.info("Columns have been deleted in truth: {}; updating the cookie.", columns)

    val (deleted, reliant, newCookie) = CookieOperator.deleteColumns(columns, cookie)

    if (reliant.nonEmpty) {
      log.info("Reliant computed columns will have also been deleted: {}; updating the cookie.", reliant)
      log.info("Will attempt to retry with remaining computed columns...")
    }

    cookie = newCookie

    deleted // all deleted columns and reliant computed columns that must also be deleted
  }

  private def computeColumns(targetColumns: Set[UserColumnId]): FeedbackResult = {
    if (targetColumns.nonEmpty) {
      log.info("Computing columns: {}", targetColumns)
      val columns = targetColumns.map(cookie.strategyMap(_)).flatMap(_.sourceColumnIds).toSet.toSeq

      dataCoordinatorClient.exportRows(columns, cookie) match {
        case Right(Right(RowData(pk, rows))) =>
          if (pk != cookie.primaryKey) {
            log.info(s"The primary key column has changed from ${cookie.primaryKey} to $pk; updating the cookie.")
            // this is safe because I must be caught up before a publication stage change can be made
            cookie = cookie.copy(primaryKey = pk)
          }
          val result = feedback(rows.map { row => Row(row, None) }, targetColumns)
          log.info("Done computing columns")
          result
        case Right(Left(ColumnsDoNotExist(unknown))) =>
          // since source columns must be deleted after computed columns
          // just delete those unknown columns and associated computed columns from our cookie
          // we'll get the event replayed to us later
          val deleted = deleteColumns(unknown)
          computeColumns(targetColumns -- deleted) // try to compute the un-deleted columns again
        case Left(ftddc@FailedToDiscoverDataCoordinator) =>
          log.warn("Failed to discover data-coordinator; going to request to have this dataset replayed later")
          // we will retry this "indefinitely"; do not decrement retries left
          replayLater(ftddc.english, resync = false)
        case Left(ddb@DataCoordinatorBusy) =>
          log.info("Received a 409 from data-coordinator; going to request to have this dataset replayed later")
          // we will retry this "indefinitely"; do not decrement retries left
          replayLater(ddb.english, resync = false)
        case Left(ddne@DatasetDoesNotExist) =>
          log.info("Completed updating dataset {} to version {} early: {}", datasetInternalName, cookie.dataVersion.underlying.toString, ddne.english)

          // nothing more to do here
          success(cookie)
        case Left(ue@UnexpectedError(_, cause)) =>
          log.error("Unexpected error from data-coordinator client")
          // this is unexpected, we will throw an exception and use the SW retry logic
          FeedbackError(ue.english, cause)
      }
    } else {
      success(cookie)
    }
  }
}

object CookieOperator {

  // returns all deleted columns, deleted computed columns, and the resulting cookie schema
  def deleteColumns(columns: Set[UserColumnId], cookie: CookieSchema): (Set[UserColumnId], Set[UserColumnId], CookieSchema) = {
    val reverseStrategyMap = scala.collection.mutable.Map[UserColumnId, Set[UserColumnId]]()
    cookie.strategyMap.foreach { case (target, strategy) =>
      strategy.sourceColumnIds.foreach { source =>
        reverseStrategyMap.put(source, reverseStrategyMap.getOrElse(source, Set.empty) + target)
      }
    }

    // include starting column
    def findReliantColumns(column: UserColumnId): Set[UserColumnId] = {
      val reliant = scala.collection.mutable.Set(column)

      val queue = scala.collection.mutable.Queue[UserColumnId](column)
      while (queue.nonEmpty) {
        reverseStrategyMap.getOrElse(queue.dequeue(), Set.empty).foreach { parent =>
          if (reliant.add(parent)) queue.enqueue(parent)
        }
      }

      reliant.toSet
    }

    val deleted = columns.flatMap(findReliantColumns) // includes starting columns
    val reliant = deleted.filter(cookie.strategyMap.contains)

    val resultCookie = cookie.copy(
      columnIdMap = cookie.columnIdMap -- deleted,
      strategyMap = cookie.strategyMap -- reliant
    )

    (deleted, reliant, resultCookie) // all deleted columns and reliant computed columns that must also be deleted and the resulting cookie
  }
}
