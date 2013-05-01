package com.socrata.datacoordinator
package truth.loader
package sql

import scala.collection.JavaConverters._

import java.sql.Connection
import java.util.concurrent.Executor

import gnu.trove.map.hash.TIntObjectHashMap
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.util.collection.{RowIdMap, MutableRowIdMap}
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.util.{TransferrableContextTimingReport, RowDataProvider}

final class SystemPKSqlLoader[CT, CV](_c: Connection, _p: RowPreparer[CV], _s: DataSqlizer[CT, CV], _l: DataLogger[CV], _i: RowDataProvider, _e: Executor, _tr: TransferrableContextTimingReport)
  extends
{
  // all these are early because they are all potential sources of exceptions, and I want all
  // such things in the constructor to occur _before_ the resource acquisitions in PostgresTransaction's
  // constructor.
  // so that if an OOM exception occurs the initializations in the base class are rolled back.
  private val log = SystemPKSqlLoader.log
  var jobs = new MutableRowIdMap[SystemPKSqlLoader.Operation[CV]]() // map from sid to operation
} with SqlLoader(_c, _p, _s, _l, _i, _e, _tr)
{
  import SystemPKSqlLoader._

  val systemIdColumn = datasetContext.systemIdColumn

  var pendingException: Throwable = null
  var pendingInsertResults: TIntObjectHashMap[CV] = null
  var pendingUpdateResults: TIntObjectHashMap[CV] = null
  var pendingDeleteResults: TIntObjectHashMap[CV] = null
  var pendingErrors: TIntObjectHashMap[Failure[CV]] = null

  var insertSize = 0
  var updateSize = 0
  var deleteSize = 0

  def upsert(job: Int, row: Row[CV]) {
    checkJob(job)
    row.get(systemIdColumn) match {
      case Some(systemIdValue) => // update
        if(typeContext.isNull(systemIdValue)) {
          errors.put(job, NullPrimaryKey)
        } else {
          val systemId = typeContext.makeSystemIdFromValue(systemIdValue)
          jobs.get(systemId) match {
            case None => // first job of this type
              maybeFlush()
              val op = Update(systemId, row, job, sqlizer.sizeofUpdate(row))
              jobs(systemId) = op
              updateSize += op.size
            case Some(oldJob) =>
              oldJob match {
                case Insert(insSid, oldRow, oldJob, oldSize) =>
                  assert(insSid == systemId)
                  insertSize -= oldSize
                  val newRow = datasetContext.mergeRows(oldRow, row)
                  val newOp = Insert(systemId, newRow, oldJob, sqlizer.sizeofInsert(newRow))
                  jobs(systemId) = newOp
                  insertSize += newOp.size
                  elided.put(job, (systemIdValue, oldJob))
                case Update(updSid, oldRow, oldJob, oldSize) =>
                  assert(updSid == systemId)
                  updateSize -= oldSize
                  val newRow = datasetContext.mergeRows(oldRow, row)
                  val newOp = Update(systemId, newRow, oldJob, sqlizer.sizeofUpdate(newRow))
                  jobs(systemId) = newOp
                  updateSize += newOp.size
                  elided.put(job, (systemIdValue, oldJob))
                case _: Delete =>
                  errors.put(job, NoSuchRowToUpdate(systemIdValue))
              }
          }
        }
      case None => // insert
        val systemId = idProvider.allocateId()
        val insert = Insert(systemId, row, job, sqlizer.sizeofInsert(row))
        jobs.get(systemId) match {
          case None =>
            maybeFlush()
            jobs(systemId) = insert
            insertSize += insert.size
          case Some(oldJob) =>
            oldJob match {
              case d@Delete(_, oldJob) =>
                // hey look at that, we deleted a row that didn't exist yet
                errors.put(oldJob, NoSuchRowToDelete(typeContext.makeValueFromSystemId(systemId)))
                deleteSize -= sqlizer.sizeofDelete
                jobs(systemId) = insert
                insertSize += insert.size
              case Update(_, _, oldJob, oldSize) =>
                // and we updated a row that didn't exist yet, too!
                errors.put(oldJob, NoSuchRowToUpdate(typeContext.makeValueFromSystemId(systemId)))
                updateSize -= oldSize
                jobs(systemId) = insert
                insertSize += insert.size
              case Insert(_, _, _, _) =>
                sys.error("Allocated the same row ID twice?")
            }
        }
    }
  }

  def delete(job: Int, id: CV) {
    checkJob(job)
    val systemId = typeContext.makeSystemIdFromValue(id)
    val delete = Delete(systemId, job)
    jobs.get(systemId) match {
      case None =>
        maybeFlush()
        jobs(systemId) = delete
        deleteSize += sqlizer.sizeofDelete
      case Some(oldJob) =>
        oldJob match {
          case Update(_, _, oldJob, oldSize) =>
            // delete-of-update -> we have to flush because we can't know if this row
            // actually exists.  Hopefully this won't happen a lot!
            flush()
            jobs(systemId) = delete
            deleteSize += sqlizer.sizeofDelete
          case Insert(allocatedSid, _, oldJob, oldSize) =>
            // deleting a row we just inserted?  Ok.  Let's nuke 'em!
            // Note: not de-allocating sid because we conceptually used it
            elided.put(oldJob, (id, job))
            insertSize -= oldSize
            // and we can skip actually doing this delete too, because we know it'll succeed
            deleted.put(job, id)
            jobs -= systemId // and then we need do nothing with this job
          case Delete(_, _) =>
            // two deletes in a row.... this one certainly fails
            errors.put(job, NoSuchRowToDelete(id))
        }
    }
  }

  def maybeFlush() {
    if(deleteSize >= softMaxBatchSizeInBytes || updateSize >= softMaxBatchSizeInBytes || insertSize >= softMaxBatchSizeInBytes) {
      flush()
    }
  }

  override def flush() {
    if(jobs.isEmpty) return

    timingReport("flush") {
      val started = new java.util.concurrent.Semaphore(0)

      connectionMutex.synchronized {
        checkAsyncJob()

        val currentJobs = jobs
        val currentInsertSize = insertSize
        val currentUpdateSize = updateSize
        val currentDeleteSize = deleteSize

        val ctx = timingReport.context
        executor.execute(new Runnable() {
          def run() {
            timingReport.withContext(ctx) {
              connectionMutex.synchronized {
                try {
                  started.release()

                  val deletes = new java.util.ArrayList[Delete]
                  val inserts = new java.util.ArrayList[Insert[CV]]
                  val updates = new java.util.ArrayList[Update[CV]]

                  val it = currentJobs.iterator
                  while(it.hasNext) {
                    it.advance()
                    it.value match {
                      case i@Insert(_,_,_, _) => inserts.add(i)
                      case u@Update(_,_,_, _) => updates.add(u)
                      case d@Delete(_,_) => deletes.add(d)
                    }
                  }

                  val errors = new TIntObjectHashMap[Failure[CV]]
                  pendingDeleteResults = processDeletes(currentDeleteSize, deletes, errors)
                  pendingUpdateResults = processUpdates(currentUpdateSize, updates, errors)
                  pendingInsertResults = processInserts(currentInsertSize, inserts)
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

      started.acquire()

      jobs = new MutableRowIdMap[Operation[CV]]
      insertSize = 0
      updateSize = 0
      deleteSize = 0
    }
  }

  def processDeletes(deleteSizeX: Int, deletes: java.util.ArrayList[Delete], errors: TIntObjectHashMap[Failure[CV]]): TIntObjectHashMap[CV] = {
    var deleteSize = deleteSizeX
    var resultMap: TIntObjectHashMap[CV] = null
    if(!deletes.isEmpty) {
      timingReport("process-deletes", "jobs" -> deletes.size) {
        using(connection.prepareStatement(sqlizer.prepareSystemIdDeleteStatement)) { stmt =>
          val it = deletes.iterator()
          while(it.hasNext) {
            val op = it.next()
            sqlizer.prepareSystemIdDelete(stmt, op.id)
            stmt.addBatch()
            deleteSize -= sqlizer.sizeofDelete
          }

          val results = stmt.executeBatch()
          assert(results.length == deletes.size, "Expected " + deletes.size + " results for deletes; got " + results.length)

          var i = 0
          resultMap = new TIntObjectHashMap[CV]
          do {
            val op = deletes.get(i)
            val idValue = typeContext.makeValueFromSystemId(op.id)
            if(results(i) == 1) {
              dataLogger.delete(op.id)
              resultMap.put(op.job, idValue)
            } else if(results(i) == 0) errors.put(op.job, NoSuchRowToDelete(idValue))
            else sys.error("Unexpected result code from delete: " + results(i))
            i += 1
          } while(i != results.length)
        }
      }
    }
    assert(deleteSize == 0, "No deletes, but delete size is not 0?")
    resultMap
  }

  def loadOldRows(ids: Iterator[CV]): RowIdMap[Row[CV]] = {
    timingReport("loadOldRows") {
      using(sqlizer.findRows(connection, ids)) { blocks =>
        val target = new MutableRowIdMap[Row[CV]]
        for(rowWithId <- blocks.flatten) target(rowWithId.rowId) = rowWithId.row
        target.freeze()
      }
    }
  }

  def processUpdates(updateSizeX: Int, updates: java.util.ArrayList[Update[CV]], errors: TIntObjectHashMap[Failure[CV]]): TIntObjectHashMap[CV] = {
    var updateSize = updateSizeX
    var resultMap: TIntObjectHashMap[CV] = null
    if(!updates.isEmpty) {
      val oldRows = loadOldRows(updates.iterator.asScala.map { u => typeContext.makeValueFromSystemId(u.id) })
      timingReport("process-updates", "jobs" -> updates.size) {
        using(connection.createStatement()) { stmt =>
          val it = updates.iterator()
          val updatesRun = new java.util.ArrayList[Update[CV]](updates.size)
          while(it.hasNext) {
            val op = it.next()
            oldRows.get(op.id) match {
              case Some(oldRow) =>
                op.row = rowPreparer.prepareForUpdate(op.row, oldRow = oldRow)
                val sql = sqlizer.sqlizeSystemIdUpdate(op.id, op.row)
                stmt.addBatch(sql)
                updatesRun.add(op)
              case None =>
                errors.put(op.job, NoSuchRowToUpdate(typeContext.makeValueFromSystemId(op.id)))
            }
            updateSize -= op.size
          }

          val results = stmt.executeBatch()
          assert(results.length == updatesRun.size, "Expected " + updatesRun.size() + " results for updates; got " + results.length)

          var i = 0
          resultMap = new TIntObjectHashMap[CV]
          while(i != results.length) {
            val op = updatesRun.get(i)
            val idValue = typeContext.makeValueFromSystemId(op.id)
            if(results(i) == 1) {
              dataLogger.update(op.id, op.row)
              resultMap.put(op.job, idValue)
            } else sys.error("Unexpected result code from update: " + results(i))

            i += 1
          }
        }
      }
    }
    assert(updateSize == 0, updates.size + " updates, but update size is not 0?")
    resultMap
  }

  def processInserts(insertSizeX: Int, inserts: java.util.ArrayList[Insert[CV]]): TIntObjectHashMap[CV] = {
    var insertSize = insertSizeX
    var resultMap: TIntObjectHashMap[CV] = null
    if(!inserts.isEmpty) {
      timingReport("process-inserts", "jobs" -> inserts.size) {
        val insertCount = sqlizer.insertBatch(connection) { inserter =>
          var i = 0
          do {
            val op = inserts.get(i)
            op.row = rowPreparer.prepareForInsert(op.row, op.id)
            inserter.insert(op.row)
            insertSize -= op.size
            i += 1
          } while(i != inserts.size)
        }

        assert(insertCount == inserts.size, "Expected " + inserts.size + " results for inserts; got " + insertCount)

        var i = 0
        resultMap = new TIntObjectHashMap[CV]
        do {
          val op = inserts.get(i)
          val idValue = typeContext.makeValueFromSystemId(op.id)
          dataLogger.insert(op.id, op.row)
          resultMap.put(op.job, idValue)
          i += 1
        } while(i != insertCount)
      }
    }
    assert(insertSize == 0, "No inserts, but insert size is not 0?")
    resultMap
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
}

object SystemPKSqlLoader {
  // ugh; can't refer to classOf[SystemPKSqlLoader] (as far as I can tell, because that class has an
  // early initializer block)
  val log = org.slf4j.LoggerFactory.getLogger(getClass.getName.replaceAll("\\$$", ""))

  sealed abstract class Operation[+CV] {
    def id: RowId
    def job: Int
  }

  case class Insert[CV](id: RowId, var row: Row[CV], job: Int, size: Int) extends Operation[CV]
  case class Update[CV](id: RowId, var row: Row[CV], job: Int, size: Int) extends Operation[CV]
  case class Delete(id: RowId, job: Int) extends Operation[Nothing]
}
