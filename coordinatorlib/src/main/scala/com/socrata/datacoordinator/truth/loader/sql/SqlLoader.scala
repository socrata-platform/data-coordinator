package com.socrata.datacoordinator
package truth.loader
package sql

import scala.{collection => sc}
import scala.collection.immutable.VectorBuilder

import java.sql.Connection
import java.util.concurrent.Executor

import gnu.trove.map.hash.TIntObjectHashMap
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.util._
import com.socrata.datacoordinator.util.TIntObjectHashMapWrapper
import com.socrata.datacoordinator.id.{RowVersion, ColumnId, RowId}
import com.socrata.datacoordinator.util.collection.MutableRowIdSet
import com.socrata.datacoordinator.truth.RowUserIdMap
import scala.collection.mutable

/**
 * @note After passing the `dataLogger` to this constructor, the created `SqlLoader`
 *       should be considered to own it until `report` or `close` are called.  Until
 *       that point, it may be accessed by another thread.
 */
final class SqlLoader[CT, CV](val connection: Connection,
                              val rowPreparer: RowPreparer[CV],
                              val sqlizer: DataSqlizer[CT, CV],
                              val dataLogger: DataLogger[CV],
                              val idProvider: RowDataProvider,
                              val executor: Executor,
                              val timingReport: TransferrableContextTimingReport)
  extends Loader[CV]
{
  require(!connection.getAutoCommit, "Connection is in auto-commit mode")

  val log = org.slf4j.LoggerFactory.getLogger(classOf[SqlLoader[_, _]])
  val typeContext = sqlizer.typeContext
  val datasetContext = sqlizer.datasetContext
  val isSystemPK = !datasetContext.hasUserPrimaryKey

  val softMaxBatchSizeInBytes = sqlizer.softMaxBatchSize

  private case class DeleteOp(job: Int, id: CV, version: Option[RowVersion])
  private sealed abstract class UpsertLike
  private case class KnownToBeInsertOp(job: Int, id: RowId, row: Row[CV]) extends UpsertLike
  private case class UpsertOp(job: Int, id: CV, row: Row[CV]) extends UpsertLike
  private case class UpdateOp(job: Int, sid: RowId, id: CV, newPreparedRow: Row[CV])
  private class Queues {
    val deletions = new VectorBuilder[DeleteOp]
    var deleteSize = 0L

    val upserts = new VectorBuilder[UpsertLike]
    val upsertIds = datasetContext.makeIdMap[AnyRef]()
    var upsertSize = 0L

    def isSufficientlyLarge = deleteSize + upsertSize > softMaxBatchSizeInBytes
    var isEmpty = true
  }

  private var currentBatch = new Queues

  val inserted = new TIntObjectHashMap[IdAndVersion[CV]]
  val updated = new TIntObjectHashMap[IdAndVersion[CV]]
  val deleted = new TIntObjectHashMap[CV]
  val errors = new TIntObjectHashMap[Failure[CV]]

  private var pendingException: Throwable = null
  private var pendingErrors: TIntObjectHashMap[Failure[CV]] = null
  private val connectionMutex = new Object
  def checkAsyncJob() {
    connectionMutex.synchronized {
      if(pendingException != null) {
        val e = pendingException
        pendingException = null
        throw e
      }

      if(pendingErrors != null) { errors.putAll(pendingErrors); pendingErrors = null }
    }
  }

  var lastJobNum = -1

  def checkJob(num: Int) {
    if(num > lastJobNum) lastJobNum = num
    else throw new IllegalArgumentException("Job numbers must be strictly increasing")
  }

  def flush() {
    if(currentBatch.isEmpty) return

    timingReport("flush") {
      val started = new java.util.concurrent.Semaphore(0)

      connectionMutex.synchronized {
        checkAsyncJob()

        assert(pendingErrors == null, "pendingErrors is set at start of worker queue run?")
        pendingErrors = new TIntObjectHashMap[Failure[CV]]
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

  def maybeFlush() {
    if(currentBatch.isSufficientlyLarge) flush()
  }

  def report: Report[CV] = {
    flush()

    connectionMutex.synchronized {
      checkAsyncJob()

      def w[T](x: TIntObjectHashMap[T]) = TIntObjectHashMapWrapper(x)
      new SqlLoader.JobReport[CV](w(inserted), w(updated), w(deleted), w(errors))
    }
  }

  def close() {
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

  def idOf(row: Row[CV]): Option[CV] = // None if null or not present
    getRejectingNull(row, datasetContext.primaryKeyColumn)

  def versionOf(row: Row[CV]): Option[RowVersion] = // None if null or not present
    getRejectingNull(row, datasetContext.versionColumn).map(typeContext.makeRowVersionFromValue)

  def upsert(jobId: Int, row: Row[CV]) {
    checkJob(jobId)
    idOf(row) match {
      case Some(id) =>
        currentBatch.upserts += UpsertOp(jobId, id, row)
        currentBatch.upsertSize += sqlizer.sizeof(row)
        currentBatch.upsertIds.put(id, this)
        currentBatch.isEmpty = false
      case None if isSystemPK =>
        versionOf(row) match {
          case None =>
            val newSid = idProvider.allocateId()
            currentBatch.upserts += KnownToBeInsertOp(jobId, newSid, row)
            currentBatch.upsertSize += sqlizer.sizeof(row)
            currentBatch.upsertIds.put(typeContext.makeValueFromSystemId(newSid), this)
            currentBatch.isEmpty = false
          case Some(v) =>
            errors.put(jobId, VersionOnNewRow)
        }
      case None =>
        errors.put(jobId, NoPrimaryKey)
    }
    maybeFlush()
  }

  def delete(jobId: Int, id: CV, version: Option[RowVersion]) {
    checkJob(jobId)
    if(currentBatch.upsertIds.contains(id)) {
      log.debug("Delete forced a flush; potential pipeline stall")
      flush()
    }
    currentBatch.deletions += DeleteOp(jobId, id, version)
    currentBatch.deleteSize += sqlizer.sizeofDelete(id)
    currentBatch.isEmpty = false
    maybeFlush()
  }

  private def process(batch: Queues) {
    processDeletes(batch.deletions.result())
    val updates = processInserts(batch.upserts.result())
    processUpdates(updates)
  }

  def lookupIdsAndVersions(ids: Iterator[CV]): RowUserIdMap[CV, InspectedRowless[CV]] =
    timingReport("lookup-ids-and-versions") {
      using(sqlizer.findIdsAndVersions(connection, ids)) { it =>
        val result = datasetContext.makeIdMap[InspectedRowless[CV]]()
        for(rowless <- it.flatten) result.put(rowless.id, rowless)
        result
      }
    }

  def lookupRows(ids: Iterator[CV]): RowUserIdMap[CV, InspectedRow[CV]] =
    timingReport("lookup-rows") {
      using(sqlizer.findRows(connection, ids)) { it =>
        val result = datasetContext.makeIdMap[InspectedRow[CV]]()
        for(row <- it.flatten) result.put(row.id, row)
        result
      }
    }

  private def processDeletes(deletes: Seq[DeleteOp]) {
    if(deletes.nonEmpty) {
      val existingIdsAndVersions = lookupIdsAndVersions(deletes.iterator.map(_.id))
      val completedDeletions = new mutable.ArrayBuffer[(RowId, Int, CV)](deletes.size)
      val (deletedCount, ()) = sqlizer.deleteBatch(connection) { deleter =>
        for(delete <- deletes) {
          existingIdsAndVersions.get(delete.id) match {
            case Some(InspectedRowless(_, sid, version)) =>
              rowPreparer.prepareForDelete(delete.id, delete.version, version) match {
                case None =>
                  deleter.delete(sid)
                  completedDeletions += ((sid, delete.job, delete.id))
                  existingIdsAndVersions.remove(delete.id)
                case Some(err) =>
                  pendingErrors.put(delete.job, err)
              }
            case None =>
              pendingErrors.put(delete.job, NoSuchRowToDelete(delete.id))
          }
        }
      }
      assert(deletedCount == completedDeletions.size, "Didn't delete as many rows as I thought it would?")
      for((sid, job, id) <- completedDeletions) {
        dataLogger.delete(sid)
        deleted.put(job, id)
      }
    }
  }

  private def processInserts(upserts: Seq[UpsertLike]): Vector[UpdateOp] = {
    val knownUpdates = new VectorBuilder[UpdateOp]
    if(upserts.nonEmpty) {
      val completedInserts = new mutable.ArrayBuffer[(Int, InspectedRow[CV])]
      val knownRows = datasetContext.makeIdMap[InspectedRow[CV]]() // The set of rows we are certain exists
      var believedInserted = 0
      val (insertedCount, remainingUpsertsToTry) =
        sqlizer.insertBatch(connection) { inserter =>
          if(isSystemPK) {
            val possiblyUpdates = datasetContext.makeIdMap[VectorBuilder[UpsertOp]]()
            var foundKnownToBeInsert = false
            upserts foreach {
              case KnownToBeInsertOp(job, sid, row) =>
                foundKnownToBeInsert = true
                rowPreparer.prepareForInsert(row, sid) match {
                  case Right(preparedRow) =>
                    val sidValue = preparedRow(datasetContext.systemIdColumn)
                    assert(typeContext.makeSystemIdFromValue(sidValue) == sid, "preparing the row for insert put the wrong sid in?")
                    val version = typeContext.makeRowVersionFromValue(preparedRow(datasetContext.versionColumn))

                    inserter.insert(preparedRow)
                    believedInserted += 1

                    val inspectedRow = InspectedRow(sidValue, sid, version, preparedRow)
                    completedInserts += job -> inspectedRow
                    knownRows.put(sidValue, inspectedRow)

                    possiblyUpdates.get(sidValue) match {
                      case Some(updates) =>
                        // These updates happened before the relevant insert.  Kill 'em!
                        for(update <- updates.result()) {
                          pendingErrors.put(update.job, NoSuchRowToUpdate(update.id))
                        }
                        possiblyUpdates.remove(sidValue)
                      case None =>
                        // ok good
                    }
                  case Left(err) =>
                    pendingErrors.put(job, err)
                }
              case u@UpsertOp(_, id, _) =>
                val updatesForId = possiblyUpdates.get(id) match {
                  case Some(builder) =>
                    builder
                  case None =>
                    val b = new VectorBuilder[UpsertOp]
                    possiblyUpdates.put(id, b)
                    b
                }
                updatesForId += u
            }
            if(foundKnownToBeInsert) possiblyUpdates.valuesIterator.flatMap(_.result().iterator).toVector
            else {
              assert(upserts.forall(_.isInstanceOf[UpsertOp]), "Found non-UpsertOp in upserts, but there were no KnownToBeInserts?")
              upserts.asInstanceOf[Seq[UpsertOp]]
            }
          } else {
            assert(upserts.forall(_.isInstanceOf[UpsertOp]), "Found non-UpsertOp in upserts, but it's not a system PK dataset?")
            upserts.asInstanceOf[Seq[UpsertOp]]
          }
        }
      assert(insertedCount == knownRows.size, "Insert count is different from the number of rows collected")
      val preExistingRows = lookupRows(remainingUpsertsToTry.iterator.map(_.id).filterNot(knownRows.contains))
      val (secondaryInsertedCount, ()) = sqlizer.insertBatch(connection) { inserter =>
        for(update <- remainingUpsertsToTry) {
          knownRows.get(update.id) match {
            case Some(previouslyInserted) => // An update, because we just inserted it.
              rowPreparer.prepareForUpdate(update.row, oldRow = previouslyInserted.row) match {
                case Right(preparedRow) =>
                  knownUpdates += UpdateOp(update.job, previouslyInserted.rowId, update.id, preparedRow)
                  val version = typeContext.makeRowVersionFromValue(preparedRow(datasetContext.versionColumn))
                  knownRows.put(update.id, InspectedRow(update.id, previouslyInserted.rowId, version, preparedRow))
                case Left(err) =>
                  pendingErrors.put(update.job, err)
              }
            case None =>
              preExistingRows.get(update.id) match {
                case Some(existing) =>
                  rowPreparer.prepareForUpdate(update.row, oldRow = existing.row) match {
                    case Right(preparedRow) =>
                      knownUpdates += UpdateOp(update.job, existing.rowId, update.id, preparedRow)
                      val version = typeContext.makeRowVersionFromValue(preparedRow(datasetContext.versionColumn))
                      knownRows.put(update.id, InspectedRow(update.id, existing.rowId, version, preparedRow))
                    case Left(err) =>
                      pendingErrors.put(update.job, err)
                  }
                case None if isSystemPK =>
                  pendingErrors.put(update.job, NoSuchRowToUpdate(update.id))
                case None =>
                  // yay it's actually an insert
                  val sid = idProvider.allocateId()
                  rowPreparer.prepareForInsert(update.row, sid) match {
                    case Right(preparedRow) =>
                      inserter.insert(preparedRow)
                      believedInserted += 1

                      val version = typeContext.makeRowVersionFromValue(preparedRow(datasetContext.versionColumn))
                      val inspectedRow = InspectedRow(update.id, sid, version, preparedRow)
                      knownRows.put(update.id, inspectedRow)
                      completedInserts += update.job -> inspectedRow
                    case Left(err) =>
                      pendingErrors.put(update.job, err)
                  }
              }
          }
        }
      }
      assert(secondaryInsertedCount + insertedCount == believedInserted, s"Insert count ($secondaryInsertedCount + $insertedCount) is different from the TOTAL number of rows collected (${believedInserted})")

      for((job, InspectedRow(id, sid, version, row)) <- completedInserts) {
        dataLogger.insert(sid, row)
        inserted.put(job, IdAndVersion(id, version))
      }
    }
    knownUpdates.result()
  }

  def processUpdates(updates: Seq[UpdateOp]) {
    if(updates.nonEmpty) {
      timingReport("process-updates", "jobs" -> updates.size) {
        using(connection.prepareStatement(sqlizer.prepareSystemIdUpdateStatement)) { stmt =>
          for(update <- updates) {
            sqlizer.prepareSystemIdUpdate(stmt, update.sid, update.newPreparedRow)
            stmt.addBatch()
          }
          val affected = stmt.executeBatch()
          assert(affected.length == updates.length, "Didn't execute as many statements as expected?")
          assert(affected.forall(_ == 1L), "At least one update didn't affect any row?")
        }
      }
      for(update <- updates) {
        dataLogger.update(update.sid, update.newPreparedRow)
        val version = typeContext.makeRowVersionFromValue(update.newPreparedRow(datasetContext.versionColumn))
        updated.put(update.job, IdAndVersion(update.id, version))
      }
    }
  }
}

object SqlLoader {
  def apply[CT, CV](connection: Connection, preparer: RowPreparer[CV], sqlizer: DataSqlizer[CT, CV], dataLogger: DataLogger[CV], idProvider: RowDataProvider, executor: Executor, timingReport: TransferrableContextTimingReport): SqlLoader[CT,CV] = {
    new SqlLoader(connection, preparer, sqlizer, dataLogger, idProvider, executor, timingReport)
  }

  case class JobReport[CV](inserted: sc.Map[Int, IdAndVersion[CV]], updated: sc.Map[Int, IdAndVersion[CV]], deleted: sc.Map[Int, CV], errors: sc.Map[Int, Failure[CV]]) extends Report[CV]
}
