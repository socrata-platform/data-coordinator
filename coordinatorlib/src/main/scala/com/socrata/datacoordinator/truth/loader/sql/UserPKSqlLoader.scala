package com.socrata.datacoordinator
package truth.loader
package sql

import java.sql.Connection
import java.util.concurrent.Executor

import gnu.trove.map.hash.TIntObjectHashMap
import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.util.IdProviderPool

final class UserPKSqlLoader[CT, CV](_c: Connection, _tc: TypeContext[CV], _s: DataSqlizer[CT, CV], _i: IdProviderPool, _e: Executor)
  extends
{
  // all these are early because they are all potential sources of exceptions, and I want all
  // such things in the constructor to occur _before_ the resource acquisitions in PostgresTransaction's
  // constructor.
  private val log = UserPKSqlLoader.log
  val primaryKey = _s.datasetContext.userPrimaryKeyColumn.getOrElse(sys.error("Created a UserPKPostgresTranasction but didn't have a user PK"))
  var jobs = _s.datasetContext.makeIdMap[UserPKSqlLoader.OperationLog[CV]]()
} with SqlLoader(_c, _tc, _s, _i, _e)
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

  def upsert(row: Row[CV]) {
    val job = nextJobNum()
    row.get(primaryKey) match {
      case Some(userId) =>
        if(typeContext.isNull(userId))
          errors.put(job, NullPrimaryKey)
        else checkNoSystemColumns(row) match {
          case None =>
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
          case Some(error) =>
            errors.put(job, error)
        }
      case None =>
        errors.put(job, NoPrimaryKey)
    }
  }

  def delete(id: CV) {
    val job = nextJobNum()
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

  def checkNoSystemColumns(row: Row[CV]): Option[Failure[CV]] = {
    val systemColumns = datasetContext.systemColumns(row)
    if(systemColumns.isEmpty) None
    else Some(SystemColumnsSet(systemColumns))
  }

  def flush() {
    if(jobs.isEmpty) return

    val started = new java.util.concurrent.Semaphore(0)

    connectionMutex.synchronized {

      checkAsyncJob()

      val currentJobs = jobs
      val currentInsertSize = insertSize
      val currentDeleteSize = deleteSize

      executor.execute(new Runnable() {
        def run() {
          connectionMutex.synchronized {
            try {
              started.release()

              val sidsForUpdateAndDelete = findSids(currentJobs.valuesIterator)

              val deletes = new java.util.ArrayList[OperationLog[CV]]
              val inserts = new java.util.ArrayList[OperationLog[CV]]
              val updates = new java.util.ArrayList[OperationLog[CV]]

              var remainingInsertSize = currentInsertSize

              currentJobs.foreach { (_, op) =>
                if(op.hasDeleteJob) { deletes.add(op) }
                if(op.hasUpsertJob) {
                  if(sidsForUpdateAndDelete.contains(op.id) && !op.forceInsert) {
                    remainingInsertSize -= op.upsertSize
                    updates.add(op)
                  } else {
                    inserts.add(op)
                  }
                }
              }

              val errors = new TIntObjectHashMap[Failure[CV]]
              pendingDeleteResults = processDeletes(sidsForUpdateAndDelete, currentDeleteSize, deletes, errors)
              pendingInsertResults = processInserts(remainingInsertSize, inserts)
              pendingUpdateResults = processUpdates(sidsForUpdateAndDelete, updates)
              if(!errors.isEmpty) pendingErrors = errors
            } catch {
              case e: Throwable =>
                pendingException = e
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

  def findSids(ops: Iterator[OperationLog[CV]]): RowIdMap[CV, Long] = {
    val target = datasetContext.makeIdMap[Long]()
    using(connection.createStatement()) { stmt =>
      var blockCount = 0
      for(sql <- sqlizer.findSystemIds(ops.map(_.id))) {
        blockCount += 1
        for {
          rs <- managed(stmt.executeQuery(sql))
          idPair <- sqlizer.extractIdPairs(rs)
        } target.put(idPair.userId, idPair.systemId)
      }
      log.debug("Looked up {} ID(s) in {} block(s)", target.size, blockCount)
    }
    target
  }

  def processDeletes(sidSource: RowIdMap[CV, Long], deleteSizeX: Int, deletes: java.util.ArrayList[OperationLog[CV]], errors: TIntObjectHashMap[Failure[CV]]): TIntObjectHashMap[CV] = {
    var deleteSize = deleteSizeX
    var resultMap: TIntObjectHashMap[CV] = null
    if(!deletes.isEmpty) {
      using(connection.prepareStatement(sqlizer.prepareUserIdDeleteStatement)) { stmt =>
        val it = deletes.iterator()
        while(it.hasNext) {
          val op = it.next()
          assert(op.hasDeleteJob, "No delete job?")
          sqlizer.prepareUserIdDelete(stmt, op.id)
          stmt.addBatch()
          deleteSize -= sqlizer.sizeofDelete
        }

        val results = stmt.executeBatch()
        assert(results.length == deletes.size, "Expected " + deletes.size + " results for deletes; got " + results.length)

        var i = 0
        resultMap = new TIntObjectHashMap[CV]
        do {
          val op = deletes.get(i)
          if(results(i) == 1 || op.forceDeleteSuccess) {
            sidSource.get(op.id) match {
              case Some(sid) => rowAuxData.delete(sid)
              case None if op.forceDeleteSuccess => // ok
              case None => sys.error("Successfully deleted row, but no sid found for it?")
            }
            resultMap.put(op.deleteJob, op.id)
          } else if(results(i) == 0) errors.put(op.deleteJob, NoSuchRowToDelete(op.id))
          else sys.error("Unexpected result code from delete: " + results(i))
          i += 1
        } while(i != results.length)
      }
    }
    assert(deleteSize == 0, "No deletes, but delete size is not 0?")
    resultMap
  }

  def processInserts(insertSizeX: Int, inserts: java.util.ArrayList[OperationLog[CV]]): TIntObjectHashMap[CV] = {
    var insertSize = insertSizeX
    var resultMap: TIntObjectHashMap[CV] = null
    if(!inserts.isEmpty) {
      val sids = new Array[Long](inserts.size)
      val insertedCount = sqlizer.insertBatch(connection) { inserter =>
        var i = 0
        do {
          val op = inserts.get(i)
          assert(op.hasUpsertJob, "No upsert job?")
          val sid = idProvider.allocate()
          sids(i) = sid
          inserter.insert(sid, op.upsertedRow)
          insertSize -= op.upsertSize
          i += 1
        } while(i != inserts.size)
      }
      assert(insertedCount == inserts.size, "Expected " + inserts.size + " results for inserts; got " + insertedCount)

      var i = inserts.size
      resultMap = new TIntObjectHashMap[CV]
      do {
        i -= 1
        val op = inserts.get(i)
        rowAuxData.insert(sids(i), op.upsertedRow)
        resultMap.put(op.upsertJob, op.id)
      } while(i != 0)
    }
    assert(insertSize == 0, "No inserts, but insert size is not 0?  Instead it's " + insertSize)
    resultMap
  }

  def processUpdates(sidSource: RowIdMap[CV, Long], updates: java.util.ArrayList[OperationLog[CV]]): TIntObjectHashMap[CV] = {
    var resultMap: TIntObjectHashMap[CV] = null
    if(!updates.isEmpty) {
      using(connection.createStatement()) { stmt =>
        var i = 0
        do {
          val op = updates.get(i)
          assert(op.hasUpsertJob, "No upsert job?")

          if(sidSource.contains(op.id)) {
            val sql = sqlizer.sqlizeUserIdUpdate(op.upsertedRow)
            stmt.addBatch(sql)
          } else {
            sys.error("Update requested but no system id found?")
          }

          i += 1
        } while(i != updates.size)

        val results = stmt.executeBatch()
        assert(results.length == updates.size, "Expected " + updates.size + " results for updates; got " + results.length)

        i = 0
        resultMap = new TIntObjectHashMap[CV]
        do {
          val op = updates.get(i)
          if(results(i) == 1) {
            val sid = sidSource.get(op.id).getOrElse(sys.error("Successfully updated row, but no sid found for it?"))
            rowAuxData.update(sid, op.upsertedRow)
            resultMap.put(op.upsertJob, op.id)
          } else if(results(i) == 0) {
            sys.error("Expected update to succeed")
          } else sys.error("Unexpected result code from update: " + results(i))

          i += 1
        } while(i != results.size)
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
    var upsertedRow: Row[CV] = null
    var upsertSize: Int = -1
    var forceInsert: Boolean = false

    def clearDelete() {
      deleteJob = -1
      forceDeleteSuccess = false
    }

    def clearUpsert() {
      upsertJob = -1
      upsertedRow = null
      upsertSize = -1
      forceInsert = false
    }

    def hasDeleteJob = deleteJob != -1
    def hasUpsertJob = upsertJob != -1
  }
}
