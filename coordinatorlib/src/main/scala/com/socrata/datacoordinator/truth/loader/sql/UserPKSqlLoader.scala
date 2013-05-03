package com.socrata.datacoordinator
package truth.loader
package sql

import java.sql.Connection
import java.util.concurrent.Executor

import gnu.trove.map.hash.TIntObjectHashMap
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.truth.RowUserIdMap
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.util.{TransferrableContextTimingReport, RowDataProvider}

final class UserPKSqlLoader[CT, CV](_c: Connection, _p: RowPreparer[CV], _s: DataSqlizer[CT, CV], _l: DataLogger[CV], _i: RowDataProvider, _e: Executor, _tr: TransferrableContextTimingReport)
  extends
{
  // all these are early because they are all potential sources of exceptions, and I want all
  // such things in the constructor to occur _before_ the resource acquisitions in PostgresTransaction's
  // constructor.
  private val log = UserPKSqlLoader.log
  val primaryKey = _s.datasetContext.userPrimaryKeyColumn.getOrElse(sys.error("Created a UserPKPostgresTranasction but didn't have a user PK"))
  var jobs = _s.datasetContext.makeIdMap[UserPKSqlLoader.OperationLog[CV]]()
} with SqlLoader(_c, _p, _s, _l, _i, _e, _tr)
{
  import UserPKSqlLoader._

  var pendingException: Throwable = null
  var pendingInsertResults: TIntObjectHashMap[CV] = null
  var pendingUpdateResults: TIntObjectHashMap[CV] = null
  var pendingDeleteResults: TIntObjectHashMap[CV] = null
  var pendingErrors: TIntObjectHashMap[Failure[CV]] = null

  var insertSize = 0
  var deleteSize = 0

  def jobEntry(id: CV) = {
    jobs.get(id) match {
      case Some(record) =>
        record
      case None =>
        maybeFlush()
        val record = new OperationLog[CV]
        record.id = id
        jobs.put(id, record)
        record
    }
  }

  def maybeFlush() {
    if(insertSize >= softMaxBatchSizeInBytes || deleteSize >= softMaxBatchSizeInBytes) {
      if(log.isDebugEnabled) {
        log.debug("Flushing due to exceeding batch size: {} ({} jobs)", Array[Any](insertSize, deleteSize), jobs.size : Any)
      }
      flush()
    }
  }

  def upsert(job: Int, row: Row[CV]) {
    checkJob(job)
    row.get(primaryKey) match {
      case Some(userId) =>
        if(typeContext.isNull(userId))
          errors.put(job, NullPrimaryKey)
        else {
          val record = jobEntry(userId)
          if(record.hasUpsertJob) {
            val oldSize = record.upsertSize
            record.upsertedRow = datasetContext.mergeRows(record.upsertedRow, row)
            record.upsertSize = sqlizer.sizeofInsert(record.upsertedRow)
            insertSize += record.upsertSize - oldSize

            elided.put(job, (userId, record.upsertJob))
          } else {
            record.upsertedRow = row
            record.upsertJob = job
            record.upsertSize = sqlizer.sizeofInsert(row)
            insertSize += record.upsertSize

            if(record.hasDeleteJob)
              record.forceInsert = true
          }
        }
      case None =>
        errors.put(job, NoPrimaryKey)
    }
  }

  def delete(job: Int, id: CV) {
    checkJob(job)
    val record = jobEntry(id)

    if(record.hasUpsertJob) {
      // ok, we'll elide the existing upsert job
      elided.put(record.upsertJob, (id, job))

      if(record.hasDeleteJob) {
        // there was a pending deletion before that upsert so we can
        // just call _this_ deletion a success.
        assert(record.forceInsert, "delete-upsert-delete but no force-insert?")
        deleted.put(job, id)
      } else {
        // No previous deletion; that upsert may or may not have been
        // an insert, but in either case by the time it got to us, there
        // was something there.  So register the delete job to do and tell
        // the task processor to ignore any failures (which means that the
        // "upsert" was really an insert)
        record.deleteJob = job
        record.forceDeleteSuccess = true
        deleteSize += sqlizer.sizeofDelete
      }

      insertSize -= record.upsertSize
      record.clearUpsert()
    } else {
      if(record.hasDeleteJob) {
        errors.put(job, NoSuchRowToDelete(id))
      } else {
        record.deleteJob = job
        deleteSize += sqlizer.sizeofDelete
      }
    }
  }

  def flush() {
    if(jobs.isEmpty) return
    timingReport("flush") {
      val started = new java.util.concurrent.Semaphore(0)

      connectionMutex.synchronized {

        checkAsyncJob()

        val currentJobs = jobs
        val currentInsertSize = insertSize
        val currentDeleteSize = deleteSize

        val ctx = timingReport.context
        executor.execute(new Runnable() {
          def run() {
            timingReport.withContext(ctx) {
              connectionMutex.synchronized {
                try {
                  started.release()

                  val sidsForDelete = findSids(currentJobs.valuesIterator.filter(_.hasDeleteJob))
                  val rowsForUpdate = findRows(currentJobs.valuesIterator.filter(_.hasUpsertJob))

                  val deletes = new java.util.ArrayList[OperationLog[CV]]
                  val inserts = new java.util.ArrayList[OperationLog[CV]]
                  val updates = new java.util.ArrayList[OperationLog[CV]]

                  var remainingInsertSize = currentInsertSize

                  currentJobs.foreach { (_, op) =>
                    if(op.hasDeleteJob) { deletes.add(op) }
                    if(op.hasUpsertJob) {
                      if(rowsForUpdate.contains(op.id) && !op.forceInsert) {
                        remainingInsertSize -= op.upsertSize
                        updates.add(op)
                      } else {
                        inserts.add(op)
                      }
                    }
                  }

                  val errors = new TIntObjectHashMap[Failure[CV]]
                  pendingDeleteResults = processDeletes(sidsForDelete, currentDeleteSize, deletes, errors)
                  pendingInsertResults = processInserts(remainingInsertSize, inserts)
                  pendingUpdateResults = processUpdates(rowsForUpdate, updates)
                  if(!errors.isEmpty) pendingErrors = errors
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

      jobs = datasetContext.makeIdMap[OperationLog[CV]]()
      insertSize = 0
      deleteSize = 0
    }
  }

  def checkAsyncJob() {
    if(pendingException != null) {
      val e = pendingException
      pendingException = null
      throw e
    }

    if(pendingInsertResults != null) { inserted.putAll(pendingInsertResults); pendingInsertResults = null }
    if(pendingUpdateResults != null) { updated.putAll(pendingUpdateResults); pendingUpdateResults = null }
    if(pendingDeleteResults != null) { deleted.putAll(pendingDeleteResults); pendingDeleteResults = null }
    if(pendingErrors != null) { errors.putAll(pendingErrors); pendingErrors = null }
  }

  def findSids(ops: Iterator[OperationLog[CV]]): RowUserIdMap[CV, RowId] = {
    timingReport("findsids") {
      using(sqlizer.findSystemIds(connection, ops.map(_.id))) { blocks =>
        val target = datasetContext.makeIdMap[RowId]()
        for(idPair <- blocks.flatten) target.put(idPair.userId, idPair.systemId)
        target
      }
    }
  }

  def findRows(ops: Iterator[OperationLog[CV]]): RowUserIdMap[CV, RowWithId[CV]] = {
    timingReport("findrows") {
      using(sqlizer.findRows(connection, ops.map(_.id))) { blocks =>
        val target = datasetContext.makeIdMap[RowWithId[CV]]()
        for(rowWithId <- blocks.flatten) target.put(rowWithId.row(datasetContext.primaryKeyColumn), rowWithId)
        target
      }
    }
  }

  def processDeletes(sidSource: RowUserIdMap[CV, RowId], deleteSizeX: Int, deletes: java.util.ArrayList[OperationLog[CV]], errors: TIntObjectHashMap[Failure[CV]]): TIntObjectHashMap[CV] = {
    var deleteSize = deleteSizeX
    var resultMap: TIntObjectHashMap[CV] = null
    if(!deletes.isEmpty) {
      timingReport("process-deletes", "jobs" -> deletes.size) {
        resultMap = new TIntObjectHashMap[CV]
        var skipped = 0
        val deleted = sqlizer.deleteBatch(connection) { deleter =>
          var i = 0
          do {
            val op = deletes.get(i)
            assert(op.hasDeleteJob, "No delete job?")
            sidSource.get(op.id) match {
              case Some(sid) =>
                deleter.delete(sid)
                dataLogger.delete(sid)
                resultMap.put(op.deleteJob, op.id)
              case None =>
                if(op.forceDeleteSuccess) resultMap.put(op.deleteJob, op.id)
                else errors.put(op.deleteJob, NoSuchRowToDelete(op.id))
                skipped += 1
            }
            deleteSize -= sqlizer.sizeofDelete
            i += 1
          } while(i != deletes.size())
        }
        assert(deleted == deletes.size - skipped, "Deleted incorrect number of rows; expected " + (deletes.size - skipped) + " but got " + deleted)
      }
    }
    assert(deleteSize == 0, "No deletes, but delete size is not 0?")
    resultMap
  }

  def processInserts(insertSizeX: Int, inserts: java.util.ArrayList[OperationLog[CV]]): TIntObjectHashMap[CV] = {
    var insertSize = insertSizeX
    var resultMap: TIntObjectHashMap[CV] = null
    if(!inserts.isEmpty) {
      timingReport("process-inserts", "jobs" -> inserts.size) {
        val inserted = new java.util.ArrayList[(RowId, OperationLog[CV])]
        val insertedCount = sqlizer.insertBatch(connection) { inserter =>
          var i = 0
          do {
            val op = inserts.get(i)
            assert(op.hasUpsertJob, "No upsert job?")
            val sid = idProvider.allocateId()
            rowPreparer.prepareForInsert(op.upsertedRow, sid) match {
              case Right(insertRow) =>
                op.upsertedRow = insertRow
                inserter.insert(op.upsertedRow)
                insertSize -= op.upsertSize
                inserted.add((sid, op))
              case Left(err) =>
                errors.put(op.upsertJob, err)
            }
            i += 1
          } while(i != inserts.size)
        }
        assert(insertedCount == inserted.size, "Expected " + inserted.size + " results for inserts; got " + insertedCount)

        var i = 0
        resultMap = new TIntObjectHashMap[CV]
        while(i != inserted.size) {
          val (sid, op) = inserted.get(i)
          dataLogger.insert(sid, op.upsertedRow)
          resultMap.put(op.upsertJob, op.id)
          i += 1
        }
      }
    }
    assert(insertSize == 0, "No inserts, but insert size is not 0?  Instead it's " + insertSize)
    resultMap
  }

  def processUpdates(rowSource: RowUserIdMap[CV, RowWithId[CV]], updates: java.util.ArrayList[OperationLog[CV]]): TIntObjectHashMap[CV] = {
    var resultMap: TIntObjectHashMap[CV] = null
    if(!updates.isEmpty) {
      timingReport("process-updates", "jobs" -> updates.size) {
        using(connection.prepareStatement(sqlizer.prepareSystemIdUpdateStatement)) { stmt =>
          var i = 0
          val updated = new java.util.ArrayList[OperationLog[CV]](updates.size)
          do {
            val op = updates.get(i)
            assert(op.hasUpsertJob, "No upsert job?")

            rowSource.get(op.id) match {
              case Some(rowWithId) =>
                rowPreparer.prepareForUpdate(op.upsertedRow, oldRow = rowWithId.row) match {
                  case Right(updateRow) =>
                    op.upsertedRow = updateRow
                    sqlizer.prepareSystemIdUpdate(stmt, rowWithId.rowId, op.upsertedRow)
                    stmt.addBatch()
                    updated.add(op)
                  case Left(err) =>
                    errors.put(op.upsertJob, err)
                }
              case None =>
                sys.error("Update requested but no system id found?")
            }

            i += 1
          } while(i != updates.size)

          val results = stmt.executeBatch()
          assert(results.length == updated.size, "Expected " + updated.size + " results for updates; got " + results.length)

          i = 0
          resultMap = new TIntObjectHashMap[CV]
          while(i != results.length) {
            val op = updated.get(i)
            if(results(i) == 1) {
              val rowWithId = rowSource.get(op.id).getOrElse(sys.error("Successfully updated row, but no sid found for it?"))
              dataLogger.update(rowWithId.rowId, op.upsertedRow)
              resultMap.put(op.upsertJob, op.id)
            } else if(results(i) == 0) {
              sys.error("Expected update to succeed")
            } else sys.error("Unexpected result code from update: " + results(i))

            i += 1
          }
        }
      }
    }
    resultMap
  }
}

object UserPKSqlLoader {
  // ugh; can't refer to classOf[UserPKSqlLoader] (as far as I can tell, because that class has an
  // early initializer block)
  val log = org.slf4j.LoggerFactory.getLogger(getClass.getName.replaceAll("\\$$", ""))

  class OperationLog[CV] {
    var id: CV = _
    var deleteJob: Int = -1
    var forceDeleteSuccess = false

    var upsertJob: Int = -1
    var upsertedRow: Row[CV] = ColumnIdMap.empty[CV]
    var upsertSize: Int = -1
    var forceInsert: Boolean = false

    def clearDelete() {
      deleteJob = -1
      forceDeleteSuccess = false
    }

    def clearUpsert() {
      upsertJob = -1
      upsertedRow = ColumnIdMap.empty[CV]
      upsertSize = -1
      forceInsert = false
    }

    def hasDeleteJob = deleteJob != -1
    def hasUpsertJob = upsertJob != -1
  }
}
