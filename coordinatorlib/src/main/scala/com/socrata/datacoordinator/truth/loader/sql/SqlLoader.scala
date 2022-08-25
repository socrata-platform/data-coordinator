package com.socrata.datacoordinator
package truth.loader
package sql

import scala.{collection => sc}
import scala.collection.immutable.VectorBuilder

import java.sql.Connection
import java.util.concurrent.Executor

import gnu.trove.map.hash.TIntObjectHashMap
import com.rojoma.simplearm.v2._

import com.socrata.datacoordinator.util._
import com.socrata.datacoordinator.util.TIntObjectHashMapWrapper
import com.socrata.datacoordinator.id.{RowVersion, ColumnId, RowId}
import com.socrata.datacoordinator.truth.RowUserIdMap
import scala.collection.mutable
import scala.util.control.ControlThrowable

/**
 * @note After passing the `dataLogger` to this constructor, the created `SqlLoader`
 *       should be considered to own it until `report` or `close` are called.  Until
 *       that point, it may be accessed by another thread.
 */
final class SqlLoader[CT, CV](val connection: Connection,
                              val rowPreparer: RowPreparer[CV],
                              val updateOnly: Boolean,
                              val sqlizer: DataSqlizer[CT, CV],
                              val dataLogger: DataLogger[CV],
                              val idProvider: RowIdProvider,
                              val versionProvider: RowVersionProvider,
                              val executor: Executor,
                              val timingReport: TransferrableContextTimingReport,
                              val reportWriter: ReportWriter[CV])
  extends Loader[CV]
{
  require(!connection.getAutoCommit, "Connection is in auto-commit mode")

  val threadId = Thread.currentThread.getId()
  val log = org.slf4j.LoggerFactory.getLogger(classOf[SqlLoader[_, _]])
  val typeContext = sqlizer.typeContext
  val datasetContext = sqlizer.datasetContext
  val isSystemPK = !datasetContext.hasUserPrimaryKey

  val softMaxBatchSizeInBytes = sqlizer.softMaxBatchSize

  private case class DeleteOp(job: Int, id: CV, version: Option[Option[RowVersion]])
  private sealed abstract class UpsertLike {
    val job: Int
    val row: Row[CV]
  }
  private case class KnownToBeInsertOp(job: Int, id: RowId, newVersion: RowVersion, row: Row[CV]) extends UpsertLike
  private case class UpsertOp(job: Int, id: CV, row: Row[CV]) extends UpsertLike

  private case class DeleteOpBySystemIdForced(job: Int, id: CV, version: Option[Option[RowVersion]])
  private case class UpsertOpBySystemIdForced(job: Int, id: CV, row: Row[CV])
  private class Queues {
    private var empty = true
    private var knownInserts = false

    private var opsByPrimaryKey = false
    private var opsForcedBySystemId = false

    private val deletionBuilder = new VectorBuilder[DeleteOp]
    private var deleteSize = 0L

    private val upsertBuilder = new VectorBuilder[UpsertLike]
    private val upsertIds = datasetContext.makeIdMap[AnyRef]()
    private var upsertSize = 0L

    def += (op: DeleteOp): Unit = {
      deletionBuilder += op
      deleteSize += sqlizer.sizeofDelete(op.id, bySystemIdForced = false)
      empty = false
      opsByPrimaryKey = true
    }

    def += (op: DeleteOpBySystemIdForced): Unit = {
      deletionBuilder += DeleteOp(op.job, op.id, op.version)
      deleteSize += sqlizer.sizeofDelete(op.id, bySystemIdForced = true)
      empty = false
      opsForcedBySystemId = true
    }

    def += (op: UpsertOp): Unit = {
      addUpsert(op.id, op)
      opsByPrimaryKey = true
    }

    def += (op: UpsertOpBySystemIdForced): Unit = {
      addUpsert(op.id, UpsertOp(op.job, op.id, op.row))
      opsForcedBySystemId = true
    }

    def += (op: KnownToBeInsertOp): Unit = {
      addUpsert(typeContext.makeValueFromSystemId(op.id), op)
      knownInserts = true
    }

    private def addUpsert(id: CV, op: UpsertLike): Unit = {
      upsertBuilder += op
      upsertIds.put(id, this)
      upsertSize += sqlizer.sizeof(op.row)
      empty = false
    }

    def isSufficientlyLarge: Boolean = deleteSize + upsertSize > softMaxBatchSizeInBytes
    def isEmpty: Boolean = empty
    def hasKnownInserts: Boolean = knownInserts
    def hasOpsByPrimaryKey: Boolean = opsByPrimaryKey
    def hasOpsForcedBySystemId: Boolean = opsForcedBySystemId

    def hasUpsertFor(id: CV): Boolean = upsertIds.contains(id)
    def upserts: Vector[UpsertLike] = upsertBuilder.result()
    def deletions: Vector[DeleteOp] = deletionBuilder.result()
  }

  // These are all updated only by the worker thread
  private var totalInsertCount = 0L
  private var totalUpdateCount = 0L
  private var totalDeleteCount = 0L

  private var currentBatch = new Queues

  private var stats = sqlizer.computeStatistics(connection)

  private var pendingException: Throwable = null
  private val connectionMutex = new Object
  def checkAsyncJob(): Unit = {
    connectionMutex.synchronized {
      if(pendingException != null) {
        val e = pendingException
        pendingException = null
        throw e
      }
    }
  }

  var lastJobNum = -1

  def checkJob(num: Int): Unit = {
    if(num > lastJobNum) lastJobNum = num
    else throw new IllegalArgumentException("Job numbers must be strictly increasing")
  }

  def flush(): Unit = {
    if(currentBatch.isEmpty) return

    timingReport("flush") {
      val started = new java.util.concurrent.Semaphore(0)

      connectionMutex.synchronized {
        checkAsyncJob()

        val newBatch = currentBatch
        val ctx = timingReport.context
        executor.execute(new Runnable() {
          def run() {
            timingReport.withContext(ctx) {
              connectionMutex.synchronized {
                try {
                  started.release()
                  process(newBatch)
                } catch {
                  case e: Throwable =>
                    pendingException = e
                }
              }
            }
          }
        })
      }
      started.acquire() // don't exit until the new job has grabbed the mutex
      currentBatch = new Queues
    }
  }

  def maybeFlush(): Unit = {
    if(currentBatch.isSufficientlyLarge) {
      log.debug("Flushing sufficiently-large batch of commands")
      flush()
    }
  }

  def finish(): Unit = {
    log.debug("Flushing batch due to being finished")
    flush()

    connectionMutex.synchronized {
      checkAsyncJob()
    }

    reportWriter.finished = true
  }

  def close(): Unit = {
    connectionMutex.synchronized {
      checkAsyncJob()
    }
  }

  def getRejectingNull(row: Row[CV], column: ColumnId): Option[CV] =
    row.get(column) match {
      case s@Some(v) =>
        if(typeContext.isNull(v)) None
        else s
      case None =>
        None
    }

  // returns value with true if the system id was returned for a dataset with a user primary key
  def idOf(row: Row[CV], bySystemId: Boolean): Option[(CV, Boolean)] = // None if null or not present
    getRejectingNull(row, datasetContext.primaryKeyColumn) match {
      case Some(id) => Some((id, false))
      case None if !isSystemPK && bySystemId => getRejectingNull(row, datasetContext.systemIdColumn).map((_, true))
      case None => None
    }

  def versionOf(row: Row[CV]): Option[Option[RowVersion]] =
    row.get(datasetContext.versionColumn) match {
      case Some(v) =>
        if(typeContext.isNull(v)) Some(None)
        else Some(Some(typeContext.makeRowVersionFromValue(v)))
      case None => None
    }

  def upsert(jobId: Int, row: Row[CV], bySystemId: Boolean): Unit = {
    checkJob(jobId)
    // okay, there are three cases here:
    // isSystemPK and bySystemId is true or false:
    //   - use system id for updates
    //   - system id not required for insert
    // isUserPK and bySystemId is false:
    //   - user user pk for updates and inserts
    // isUserPK and bySystemId is true:
    //   - allow for system id to be used on update
    //     if user pk is not passed (prefer user pk if given)

    // by_system_id only changes the behavior for datasets with user primary keys
    idOf(row, bySystemId) match {
      case Some((id, isSystemIdWhenUserPrimaryKeyExists)) =>
        // we need to flush between regular Upserts and UpsertsBySystemId
        // since we will not be able to compare the row ids
        if (isSystemIdWhenUserPrimaryKeyExists && currentBatch.hasOpsByPrimaryKey) {
          log.debug("Upsert by system id after upserts by primary key forced a flush")
          flush()
        } else if (!isSystemIdWhenUserPrimaryKeyExists && currentBatch.hasOpsForcedBySystemId) {
          log.debug("Upsert by primary key after upserts by system id forced a flush")
          flush()
        } else if (currentBatch.hasUpsertFor(id)) {
          log.debug("Upsert forced a flush; potential pipeline stall")
          flush()
        }

        if (isSystemIdWhenUserPrimaryKeyExists)
          currentBatch += UpsertOpBySystemIdForced(jobId, id, row)
        else
          currentBatch += UpsertOp(jobId, id, row)
      case None if isSystemPK =>
        versionOf(row) match {
          case None | Some(None) =>
            val newSid = idProvider.allocate()
            val newVersion = versionProvider.allocate()
            currentBatch += KnownToBeInsertOp(jobId, newSid, newVersion, row)
          case Some(Some(_)) =>
            reportWriter.error(jobId, VersionOnNewRow)
        }
      case None =>
        reportWriter.error(jobId, NoPrimaryKey)
    }
    maybeFlush()
  }

  def delete(jobId: Int, id: CV, version: Option[Option[RowVersion]], bySystemId: Boolean): Unit = {
    checkJob(jobId)

    if(typeContext.isNull(id)) {
      reportWriter.error(jobId, NoSuchRowToDelete(id))
    } else {
      // by_system_id only changes the behavior for datasets with user primary keys
      val deleteBySystemIdForced = datasetContext.hasUserPrimaryKey && bySystemId

      // if deleteBySystemIdForced is true we will assume all deletes are done by the system id column
      if (deleteBySystemIdForced && currentBatch.hasOpsByPrimaryKey) {
        log.debug("Delete by system id after upserts by primary key forced a flush")
        flush()
      } else if (!deleteBySystemIdForced && currentBatch.hasOpsForcedBySystemId) {
        log.debug("Delete by primary key after upserts by system id forced a flush")
        flush()
      } else if (currentBatch.hasUpsertFor(id)) {
        log.debug("Upsert forced a flush; potential pipeline stall")
        flush()
      }

      // we only will treat the bySystemId parameter as true when there is a user primary key column
      if (deleteBySystemIdForced)
        currentBatch += DeleteOpBySystemIdForced(jobId, id, version)
      else
        currentBatch += DeleteOp(jobId, id, version)
      maybeFlush()
    }
  }

  private def process(batch: Queues) {
    val batchForcedBySystemId = batch.hasOpsForcedBySystemId
    val batchDeleteCount = processDeletes(batch.deletions, batchForcedBySystemId)
    val (inserts, updates) = prepareInsertsAndUpdates(batch.upserts, batchForcedBySystemId)
    val batchInsertCount = doInserts(inserts)
    val batchUpdateCount = doUpdates(updates, batchForcedBySystemId)
    stats = sqlizer.updateStatistics(connection, batchInsertCount, batchDeleteCount, batchUpdateCount, stats)
  }

  private def doInserts(inserts: Seq[SqlLoader.DecoratedRow[CV]]): Long = {
    if(inserts.nonEmpty) {
      sqlizer.insertBatch(connection) { inserter =>
        for(insert <- inserts) {
          inserter.insert(insert.newRow)
          reportWriter.inserted(insert.job, IdAndVersion(insert.id, insert.version, bySystemIdForced = false))
        }
      }
      // Can't do this in the loop above because the inserter owns the DB connection for the COPY
      for(insert <- inserts) {
        dataLogger.insert(insert.rowId, insert.newRow)
      }
      val insertCount = inserts.length
      totalInsertCount += insertCount
      insertCount
    } else {
      0L
    }
  }

  private def doUpdates(updates: Seq[SqlLoader.DecoratedRow[CV]], bySystemIdForced: Boolean): Long = {
    if(updates.nonEmpty) {
      if(DebugState.isUpsertExplanationRequested(threadId)) {
        sqlizer.doExplain(
          connection,
          sqlizer.prepareSystemIdUpdateStatement,
          sqlizer.prepareSystemIdUpdate(_, updates.head.rowId, updates.head.newRow),
          idempotent = false
        )
      }
      using(connection.prepareStatement(sqlizer.prepareSystemIdUpdateStatement)) { stmt =>
        for(update <- updates) {
          sqlizer.prepareSystemIdUpdate(stmt, update.rowId, update.newRow)
          reportWriter.updated(update.job, IdAndVersion(update.id, update.version, bySystemIdForced = bySystemIdForced))
          dataLogger.update(update.rowId, Some(update.oldRow), update.newRow) // This CAN be done here because there is no active query
          stmt.addBatch()
        }
        val results = stmt.executeBatch()
        assert(results.length == updates.length, "Didn't get the same number of results as jobs in batch?")
        assert(results.forall(_ == 1), "At least one update did not affect exactly one row!")
      }
      val updateCount = updates.length
      totalUpdateCount += updateCount
      updateCount
    } else {
      0
    }
  }

  private def lookupRows(bySystemIdForced: Boolean, ids: Iterator[CV]): RowUserIdMap[CV, InspectedRow[CV]] =
    timingReport("lookup-rows") {
      using(sqlizer.findRows(connection, bySystemIdForced, ids, explain = DebugState.isUpsertExplanationRequested(threadId))) { it =>
        val result = datasetContext.makeIdMap[InspectedRow[CV]]()
        for(row <- it.flatten) result.put(row.id, row)
        result
      }
    }

  private def processDeletes(deletes: Seq[DeleteOp], bySystemIdForced: Boolean): Long = {
    if(deletes.nonEmpty) {
      var deletedCount = 0L
      for(deleteChunk <- deletes.grouped(sqlizer.findRowsBlockSize)) {
        val existingRows = lookupRows(bySystemIdForced = bySystemIdForced, deleteChunk.iterator.map(_.id))
        val completedDeletions = new mutable.ArrayBuffer[(RowId, Int, CV, Row[CV])](deleteChunk.size)
        val (chunkDeletedCount, ()) = sqlizer.deleteBatch(connection, explain = DebugState.isUpsertExplanationRequested(threadId)) { deleter =>
          for(delete <- deleteChunk) {
            existingRows.get(delete.id) match {
              case Some(InspectedRow(_, sid, version, oldRow)) =>
                checkVersion(delete.job, delete.id, delete.version, Some(version), bySystemIdForced = bySystemIdForced) {
                  deleter.delete(sid)
                  completedDeletions += ((sid, delete.job, delete.id, oldRow))
                  existingRows.remove(delete.id)
                }
              case None =>
                reportWriter.error(delete.job, NoSuchRowToDelete(delete.id))
            }
          }
        }
        assert(chunkDeletedCount == completedDeletions.size, "Didn't delete as many rows as I thought it would?")
        deletedCount += chunkDeletedCount
        for((sid, job, id, oldRow) <- completedDeletions) {
          dataLogger.delete(sid, Some(oldRow))
          reportWriter.deleted(job, id)
        }
      }
      totalDeleteCount += deletedCount
      deletedCount
    } else {
      0
    }
  }

  def checkVersion[T](job: Int,
                      id: CV,
                      newVersion: Option[Option[RowVersion]],
                      oldVersion: Option[RowVersion],
                      bySystemIdForced: Boolean)(f: => T): Unit = {
    newVersion match {
      case None => f
      case Some(v) if v == oldVersion => f
      case Some(other) =>
        reportWriter.error(job, VersionMismatch(id, oldVersion, other, bySystemIdForced = bySystemIdForced))
    }
  }

  private def prepareInsertsAndUpdates(upserts: Seq[UpsertLike], bySystemIdForced: Boolean):
    (Seq[SqlLoader.DecoratedRow[CV]], Seq[SqlLoader.DecoratedRow[CV]]) = {
    // ok, what we want to do here is divide "upserts" into two piles: inserts and updates.
    // The OUTPUT of this will be fully filled-in rows which are ready to go through the
    // validation/population script before actually being sent to the database.
    // Tricky bits:
    // * upserts that come before the relevant KnownToBeInsertOps should be instantly
    //       failed (this can only happen on sid-keyed datasets, and it means someone
    //       sent a job for a sid that didn't exist yet).  This should be SUPER RARE
    //       verging on NEVER HAPPENS.  It means that a row identifier picked out of
    //       thin air happened to be one that was generated in the same batch!
    // * Before, upserts could refer to the same ID as a previous operation.  Now this can't;
    //       a flush will have occurred if the user tries it.
    if (isSystemPK || bySystemIdForced) prepareInsertsAndUpdatesSID(upserts, bySystemIdForced)
    else prepareInsertsAndUpdatesUID(upserts)
  }

  private def prepareInsertsAndUpdatesSID(upserts: Seq[UpsertLike], bySystemIdForced: Boolean): (Seq[SqlLoader.DecoratedRow[CV]], Seq[SqlLoader.DecoratedRow[CV]]) = {
    assert(isSystemPK || bySystemIdForced)
    val inserts = Vector.newBuilder[SqlLoader.DecoratedRow[CV]]
    var unprocessedUpdates = Vector.newBuilder[UpsertOp]
    val seenOps = datasetContext.makeIdMap[UpsertOp]()

    def killPreinsertUpdate(sid: CV) {
      // As noted above, this should be SUPER RARE verging on NEVER HAPPENS.
      // This is why I'm willing to do this in this less-than-efficient way
      log.debug("Wow!  I'm killing an update that happened before the insert in a SID dataset!")
      val update = seenOps(sid)
      seenOps.remove(sid)
      reportWriter.error(update.job, NoSuchRowToUpdate(update.id, bySystemIdForced = true))
      val newUnprocessedUpdates = Vector.newBuilder[UpsertOp]
      for(upsertOp <- unprocessedUpdates.result() if upsertOp ne update) {
        newUnprocessedUpdates += upsertOp
      }
      unprocessedUpdates = newUnprocessedUpdates
    }

    val firstPassIterator = upserts.iterator
    while(firstPassIterator.hasNext) {
      // These are SIDs, so inserts will be Known and Updates will be not-known
      firstPassIterator.next() match {
        case KnownToBeInsertOp(job, sid, version, row) =>
          val preparedRow = rowPreparer.prepareForInsert(row, sid, version)
          val sidValue = preparedRow(datasetContext.systemIdColumn)
          if (seenOps.contains(sidValue)) killPreinsertUpdate(sidValue)
          if (updateOnly) reportWriter.error(job, InsertInUpdateOnly(sidValue, bySystemIdForced = bySystemIdForced))
          else inserts += SqlLoader.DecoratedRow(job, sidValue, sid, version, SqlLoader.emptyRow, preparedRow)
        case u: UpsertOp =>
          unprocessedUpdates += u
          seenOps.put(u.id, u)
      }
    }

    val updates = Vector.newBuilder[SqlLoader.DecoratedRow[CV]]
    val preexistingRows = lookupRows(bySystemIdForced = bySystemIdForced, seenOps.keysIterator)
    val secondPassIterator = unprocessedUpdates.result().iterator
    while(secondPassIterator.hasNext) {
      val op = secondPassIterator.next()
      preexistingRows.get(op.id) match {
        case Some(oldRow) =>
          checkVersion(op.job, op.id, versionOf(op.row), Some(oldRow.version), bySystemIdForced = bySystemIdForced) {
            val version = versionProvider.allocate()
            val newRow = rowPreparer.prepareForUpdate(op.row, oldRow.row, version)
            updates += SqlLoader.DecoratedRow(op.job, op.id, oldRow.rowId, version, oldRow.row, newRow)
          }
        case None =>
          reportWriter.error(op.job, NoSuchRowToUpdate(op.id, bySystemIdForced = bySystemIdForced))
      }
    }

    (inserts.result(), updates.result())
  }

  private def prepareInsertsAndUpdatesUID(upserts: Seq[UpsertLike]): (Seq[SqlLoader.DecoratedRow[CV]], Seq[SqlLoader.DecoratedRow[CV]]) = {
    assert(!isSystemPK)
    val ops = new Array[UpsertOp](upserts.length)

    var processed = 0
    val it = upserts.iterator
    while(it.hasNext) {
      it.next() match {
        case u: UpsertOp =>
          ops(processed) = u
        case _: KnownToBeInsertOp =>
          sys.error("Found KnownToBeInsertOp in a non-sid dataset")
      }
      processed += 1
    }

    val rows = lookupRows(bySystemIdForced = false, ops.iterator.map(_.id))
    val inserts = Vector.newBuilder[SqlLoader.DecoratedRow[CV]]
    val updates = Vector.newBuilder[SqlLoader.DecoratedRow[CV]]

    var i = 0
    while(i != ops.length) {
      val op = ops(i)
      rows.get(op.id) match {
        case None =>
          checkVersion(op.job, op.id, versionOf(op.row), None, bySystemIdForced = false) {
            val sid = idProvider.allocate()
            val version = versionProvider.allocate()
            val preparedRow = rowPreparer.prepareForInsert(op.row, sid, version)
            if(updateOnly) reportWriter.error(op.job, InsertInUpdateOnly(op.id, bySystemIdForced = false))
            else inserts += SqlLoader.DecoratedRow(op.job, op.id, sid, version, SqlLoader.emptyRow, preparedRow)
          }
        case Some(oldRow) =>
          checkVersion(op.job, op.id, versionOf(op.row), Some(oldRow.version), bySystemIdForced = false) {
            val newVersion = versionProvider.allocate()
            val preparedRow = rowPreparer.prepareForUpdate(op.row, oldRow.row, newVersion)
            updates += SqlLoader.DecoratedRow(op.job, op.id, oldRow.rowId, newVersion, oldRow.row, preparedRow)
          }
      }
      i += 1
    }

    (inserts.result(), updates.result())
  }
}

object SqlLoader {
  def apply[CT, CV](connection: Connection, preparer: RowPreparer[CV], updateOnly: Boolean, sqlizer: DataSqlizer[CT, CV], dataLogger: DataLogger[CV],
                    idProvider: RowIdProvider, versionProvider: RowVersionProvider, executor: Executor, reportWriter: ReportWriter[CV],
                    timingReport: TransferrableContextTimingReport): SqlLoader[CT,CV] = {
    new SqlLoader(connection, preparer, updateOnly, sqlizer, dataLogger, idProvider, versionProvider, executor, timingReport, reportWriter)
  }

  private case class DecoratedRow[CV](job: Int, id: CV, rowId: RowId, version: RowVersion, oldRow: Row[CV], newRow: Row[CV])
  private val emptyRow = Row[Nothing]()
}
