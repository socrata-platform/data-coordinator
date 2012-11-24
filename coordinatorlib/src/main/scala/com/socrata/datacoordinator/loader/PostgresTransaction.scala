package com.socrata.datacoordinator.loader

import scala.{collection => sc}

import java.sql.{Connection, PreparedStatement}
import java.util.concurrent.Executor

import com.rojoma.simplearm.util._
import gnu.trove.map.hash.TIntObjectHashMap
import gnu.trove.map.hash.TLongObjectHashMap
import com.socrata.datacoordinator.util.{TIntObjectHashMapWrapper, Counter}

abstract class PostgresTransaction[CT, CV](val connection: Connection,
                                           val typeContext: TypeContext[CV],
                                           val sqlizer: DataSqlizer[CT, CV],
                                           val idProviderPool: IdProviderPool,
                                           val executor: Executor)
  extends Transaction[CV]
{
  private val log = PostgresTransaction.log

  val datasetContext = sqlizer.datasetContext

  val softMaxBatchSizeInBytes = sqlizer.softMaxBatchSize

  val inserted = new TIntObjectHashMap[CV]
  val elided = new TIntObjectHashMap[(CV, Int)]
  val updated = new TIntObjectHashMap[CV]
  val deleted = new TIntObjectHashMap[CV]
  val errors = new TIntObjectHashMap[Failure[CV]]

  protected val connectionMutex = new Object
  def checkAsyncJob()

  val nextJobNum = new Counter

  lazy val versionNum = for {
    stmt <- managed(connection.createStatement())
    rs <- managed(stmt.executeQuery(sqlizer.findCurrentVersion))
  } yield {
    val hasNext = rs.next()
    assert(hasNext, "next version query didn't return anything?")
    rs.getLong(1) + 1
  }

  val nextSubVersionNum = new Counter(init = 1)

  object rowAuxDataState extends (sqlizer.LogAuxColumn => Unit) {
    var stmt: PreparedStatement = null

    var batched = 0
    var size = 0

    def apply(auxData: sqlizer.LogAuxColumn) {
      if(stmt == null) stmt = connection.prepareStatement(sqlizer.prepareLogRowsChangedStatement)
      size += sqlizer.prepareLogRowsChanged(stmt, versionNum, nextSubVersionNum(), auxData)
      stmt.addBatch()
      batched += 1
      if(size > sqlizer.softMaxBatchSize) flush()
    }

    def flush() {
      if(batched != 0) {
        log.debug("Flushing {} log rows", batched)
        val rs = stmt.executeBatch()
        assert(rs.length == batched)
        assert(rs.forall(_ == 1), "Inserting a log row... didn't insert a log row?")
        batched = 0
        size = 0
      }
    }

    def close() {
      if(stmt != null) stmt.close()
    }
  }
  val rowAuxData = sqlizer.newRowAuxDataAccumulator(rowAuxDataState)

  // These three initializations must run in THIS order.  Any further
  // initializations must take care to clean up after themselves if
  // they may throw!  In particular they must either early-initialize or
  // rollback the transaction and return the id provider to the pool.

  require(!connection.getAutoCommit, "Connection must be in non-auto-commit mode")

  val idProvider = idProviderPool.borrow()

  using(connection.createStatement()) { stmt =>
    var success = false
    try {
      stmt.execute(sqlizer.lockTableAgainstWrites(sqlizer.dataTableName))
      success = true
    } finally {
      if(!success) idProviderPool.release(idProvider)
    }
  }

  def flush()

  def lookup(id: CV) = {
    flush()
    for {
      stmt <- managed(connection.createStatement())
      rs <- managed(stmt.executeQuery(sqlizer.selectRow(id)))
    } yield {
      if(rs.next()) Some(sqlizer.extractRow(rs))
      else None
    }
  }

  def report: Report[CV] = {
    flush()
    connectionMutex.synchronized {
      checkAsyncJob()
      implicit def wrap[T](x: TIntObjectHashMap[T]) = TIntObjectHashMapWrapper(x)
      new PostgresTransaction.JobReport(inserted, updated, deleted, elided, errors)
    }
  }

  def commit() {
    connectionMutex.synchronized {
      checkAsyncJob()
      rowAuxData.finish()
      rowAuxDataState.flush()
    }

    flush()

    connectionMutex.synchronized {
      if(inserted != 0 || updated != 0 || deleted != 0) sqlizer.logTransactionComplete()
      connection.commit()
    }
  }

  def close() {
    connectionMutex.synchronized {
      try {
        checkAsyncJob()
      } finally {
        try {
          rowAuxDataState.close()
        } finally {
          try {
            connection.rollback()
          } finally {
            idProviderPool.release(idProvider)
          }
        }
      }
    }
  }
}

final class SystemPKPostgresTransaction[CT, CV](_c: Connection, _tc: TypeContext[CV], _s: DataSqlizer[CT, CV], _i: IdProviderPool, _e: Executor)
  extends
{
  // all these are early because they are all potential sources of exceptions, and I want all
  // such things in the constructor to occur _before_ the resource acquisitions in PostgresTransaction's
  // constructor.
  // so that if an OOM exception occurs the initializations in the base class are rolled back.
  private val log = PostgresTransaction.SystemIDOps.log
  var jobs = new TLongObjectHashMap[PostgresTransaction.SystemIDOps.Operation[CV]]() // map from sid to operation
} with PostgresTransaction(_c, _tc, _s, _i, _e)
{
  import PostgresTransaction.SystemIDOps._

  val primaryKey = datasetContext.systemIdColumnName

  var pendingException: Throwable = null
  var pendingInsertResults: TIntObjectHashMap[CV] = null
  var pendingUpdateResults: TIntObjectHashMap[CV] = null
  var pendingDeleteResults: TIntObjectHashMap[CV] = null
  var pendingErrors: TIntObjectHashMap[Failure[CV]] = null

  var insertSize = 0
  var updateSize = 0
  var deleteSize = 0

  def upsert(row: Row[CV]) {
    val job = nextJobNum()
    row.get(primaryKey) match {
      case Some(systemIdValue) => // update
        if(typeContext.isNull(systemIdValue)) {
          errors.put(job, NullPrimaryKey)
        } else checkNoSystemColumnsExceptId(row) match {
          case None =>
            val systemId = typeContext.makeSystemIdFromValue(systemIdValue)
            val oldJobNullable = jobs.get(systemId)
            if(oldJobNullable == null) { // first job of this type
              maybeFlush()
              val op = Update(systemId, row, job, sqlizer.sizeofUpdate(row))
              jobs.put(systemId, op)
              updateSize += op.size
            } else oldJobNullable match {
              case Insert(insSid, oldRow, oldJob, oldSize) =>
                assert(insSid == systemId)
                insertSize -= oldSize
                val newRow = datasetContext.mergeRows(oldRow, row)
                val newOp = Insert(systemId, newRow, oldJob, sqlizer.sizeofInsert(newRow))
                jobs.put(systemId, newOp)
                insertSize += newOp.size
                elided.put(job, (systemIdValue, oldJob))
              case Update(updSid, oldRow, oldJob, oldSize) =>
                assert(updSid == systemId)
                updateSize -= oldSize
                val newRow = datasetContext.mergeRows(oldRow, row)
                val newOp = Update(systemId, newRow, oldJob, sqlizer.sizeofUpdate(newRow))
                jobs.put(systemId, newOp)
                updateSize += newOp.size
                elided.put(job, (systemIdValue, oldJob))
              case _: Delete =>
                errors.put(job, NoSuchRowToUpdate(systemIdValue))
            }
          case Some(error) =>
            errors.put(job, error)
        }
      case None => // insert
        checkNoSystemColumnsExceptId(row) match {
          case None =>
            val systemId = idProvider.allocate()
            val oldJobNullable = jobs.get(systemId)
            val insert = Insert(systemId, row, job, sqlizer.sizeofInsert(row))
            if(oldJobNullable == null) {
              maybeFlush()
              jobs.put(systemId, insert)
              insertSize += insert.size
            } else oldJobNullable match {
              case d@Delete(_, oldJob) =>
                // hey look at that, we deleted a row that didn't exist yet
                errors.put(oldJob, NoSuchRowToDelete(typeContext.makeValueFromSystemId(systemId)))
                deleteSize -= sqlizer.sizeofDelete
                jobs.put(systemId, insert)
                insertSize += insert.size
              case Update(_, _, oldJob, oldSize) =>
                // and we updated a row that didn't exist yet, too!
                errors.put(oldJob, NoSuchRowToUpdate(typeContext.makeValueFromSystemId(systemId)))
                updateSize -= oldSize
                jobs.put(systemId, insert)
                insertSize += insert.size
              case Insert(_, _, _, _) =>
                sys.error("Allocated the same row ID twice?")
            }
          case Some(error) =>
            errors.put(job, error)
        }
    }
  }

  def delete(id: CV) {
    val job = nextJobNum()
    val systemId = typeContext.makeSystemIdFromValue(id)
    val oldJobNullable = jobs.get(systemId)
    val delete = Delete(systemId, job)
    if(oldJobNullable == null) {
      maybeFlush()
      jobs.put(systemId, delete)
      deleteSize += sqlizer.sizeofDelete
    } else oldJobNullable match {
      case Update(_, _, oldJob, oldSize) =>
        // delete-of-update -> we have to flush because we can't know if this row
        // actually exists.  Hopefully this won't happen a lot!
        flush()
        jobs.put(systemId, delete)
        deleteSize += sqlizer.sizeofDelete
      case Insert(allocatedSid, _, oldJob, oldSize) =>
        // deleting a row we just inserted?  Ok.  Let's nuke 'em!
        // Note: not de-allocating sid because we conceptually used it
        elided.put(oldJob, (id, job))
        insertSize -= oldSize
        // and we can skip actually doing this delete too, because we know it'll succeed
        deleted.put(job, id)
        jobs.remove(systemId) // and then we need do nothing with this job
      case Delete(_, _) =>
        // two deletes in a row.... this one certainly fails
        errors.put(job, NoSuchRowToDelete(id))
    }
  }

  def maybeFlush() {
    if(deleteSize >= softMaxBatchSizeInBytes || updateSize >= softMaxBatchSizeInBytes || deleteSize >= softMaxBatchSizeInBytes) {
      flush()
    }
  }

  def checkNoSystemColumnsExceptId(row: Row[CV]): Option[Failure[CV]] = {
    val systemColumns = datasetContext.systemColumns(row) - primaryKey
    if(systemColumns.isEmpty) None
    else Some(SystemColumnsSet(systemColumns))
  }

  override def flush() {
    if(jobs.isEmpty) return

    val started = new java.util.concurrent.Semaphore(0)

    connectionMutex.synchronized {
      checkAsyncJob()

      val currentJobs = jobs
      val currentInsertSize = insertSize
      val currentUpdateSize = updateSize
      val currentDeleteSize = deleteSize

      executor.execute(new Runnable() {
        def run() {
          connectionMutex.synchronized {
            try {
              started.release()

              val deletes = new java.util.ArrayList[Delete]
              val inserts = new java.util.ArrayList[Insert[CV]]
              val updates = new java.util.ArrayList[Update[CV]]

              val it = currentJobs.iterator()
              while(it.hasNext) {
                it.advance()
                it.value() match {
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
      })
    }

    started.acquire()

    jobs = new TLongObjectHashMap[Operation[CV]](jobs.capacity)
    insertSize = 0
    updateSize = 0
    deleteSize = 0
  }

  def processDeletes(deleteSizeX: Int, deletes: java.util.ArrayList[Delete], errors: TIntObjectHashMap[Failure[CV]]): TIntObjectHashMap[CV] = {
    var deleteSize = deleteSizeX
    var resultMap: TIntObjectHashMap[CV] = null
    if(!deletes.isEmpty) {
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
            rowAuxData.delete(op.id)
            resultMap.put(op.job, idValue)
          } else if(results(i) == 0) errors.put(op.job, NoSuchRowToDelete(idValue))
          else sys.error("Unexpected result code from delete: " + results(i))
          i += 1
        } while(i != results.length)
      }
    }
    assert(deleteSize == 0, "No deletes, but delete size is not 0?")
    resultMap
  }

  def processUpdates(updateSizeX: Int, updates: java.util.ArrayList[Update[CV]], errors: TIntObjectHashMap[Failure[CV]]): TIntObjectHashMap[CV] = {
    var updateSize = updateSizeX
    var resultMap: TIntObjectHashMap[CV] = null
    if(!updates.isEmpty) {
      using(connection.createStatement()) { stmt =>
        val it = updates.iterator()
        while(it.hasNext) {
          val op = it.next()
          val sql = sqlizer.sqlizeSystemIdUpdate(op.id, op.row)
          stmt.addBatch(sql)
          updateSize -= op.size
        }

        val results = stmt.executeBatch()
        assert(results.length == updates.size, "Expected " + updates.size + " results for updates; got " + results.length)

        var i = 0
        resultMap = new TIntObjectHashMap[CV]
        do {
          val op = updates.get(i)
          val idValue = typeContext.makeValueFromSystemId(op.id)
          if(results(i) == 1) {
            rowAuxData.update(op.id, op.row - datasetContext.systemIdColumnName)
            resultMap.put(op.job, idValue)
          } else if(results(i) == 0) {
            errors.put(op.job, NoSuchRowToUpdate(idValue))
          } else sys.error("Unexpected result code from update: " + results(i))

          i += 1
        } while(i != results.length)
      }
    }
    assert(updateSize == 0, updates.size + " updates, but update size is not 0?")
    resultMap
  }

  def processInserts(insertSizeX: Int, inserts: java.util.ArrayList[Insert[CV]]): TIntObjectHashMap[CV] = {
    var insertSize = insertSizeX
    var resultMap: TIntObjectHashMap[CV] = null
    if(!inserts.isEmpty) {
      val insertCount = sqlizer.insertBatch(connection) { inserter =>
        var i = 0
        do {
          val op = inserts.get(i)
          inserter.insert(op.id, op.row)
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
        rowAuxData.insert(op.id, op.row)
        resultMap.put(op.job, idValue)
        i += 1
      } while(i != insertCount)
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

final class UserPKPostgresTransaction[CT, CV](_c: Connection, _tc: TypeContext[CV], _s: DataSqlizer[CT, CV], _i: IdProviderPool, _e: Executor)
  extends
{
  // all these are early because they are all potential sources of exceptions, and I want all
  // such things in the constructor to occur _before_ the resource acquisitions in PostgresTransaction's
  // constructor.
  private val log = PostgresTransaction.UserIDOps.log
  val primaryKey = _s.datasetContext.userPrimaryKeyColumn.getOrElse(sys.error("Created a UserPKPostgresTranasction but didn't have a user PK"))
  var jobs = _s.datasetContext.makeIdMap[PostgresTransaction.UserIDOps.OperationLog[CV]]()
} with PostgresTransaction(_c, _tc, _s, _i, _e)
{
  import PostgresTransaction.UserIDOps._

  var pendingException: Throwable = null
  var pendingInsertResults: TIntObjectHashMap[CV] = null
  var pendingUpdateResults: TIntObjectHashMap[CV] = null
  var pendingDeleteResults: TIntObjectHashMap[CV] = null

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

              pendingDeleteResults = processDeletes(sidsForUpdateAndDelete, currentDeleteSize, deletes)
              pendingInsertResults = processInserts(remainingInsertSize, inserts)
              pendingUpdateResults = processUpdates(sidsForUpdateAndDelete, updates)
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

  def processDeletes(sidSource: RowIdMap[CV, Long], deleteSizeX: Int, deletes: java.util.ArrayList[OperationLog[CV]]): TIntObjectHashMap[CV] = {
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

  // Note: destroys "updates"
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

object PostgresTransaction {
  def pkg = classOf[PostgresTransaction[_, _]].getPackage.getName

  val log = org.slf4j.LoggerFactory.getLogger(classOf[PostgresTransaction[_, _]])

  def apply[CT, CV](connection: Connection, typeContext: TypeContext[CV], sqlizer: DataSqlizer[CT, CV], idProvider: IdProviderPool, executor: Executor): PostgresTransaction[CT,CV] = {
    if(sqlizer.datasetContext.hasUserPrimaryKey)
      new UserPKPostgresTransaction(connection, typeContext, sqlizer, idProvider, executor)
    else
      new SystemPKPostgresTransaction(connection, typeContext, sqlizer, idProvider, executor)
  }

  object SystemIDOps {
    val log = org.slf4j.LoggerFactory.getLogger(pkg + ".SystemPKPostgresTransaction")

    sealed abstract class Operation[+CV] {
      def id: Long
      def job: Int
    }

    case class Insert[CV](id: Long, row: Row[CV], job: Int, size: Int) extends Operation[CV]
    case class Update[CV](id: Long, row: Row[CV], job: Int, size: Int) extends Operation[CV]
    case class Delete(id: Long, job: Int) extends Operation[Nothing]

    sealed abstract class UpsertType
    case object UpsertInsert extends UpsertType
    case object UpsertUpdate extends UpsertType
  }

  object UserIDOps {
    val log = org.slf4j.LoggerFactory.getLogger(pkg + ".UserPKPostgresTransaction")

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

  case class JobReport[CV](inserted: sc.Map[Int, CV], updated: sc.Map[Int, CV], deleted: sc.Map[Int, CV], elided: sc.Map[Int, (CV, Int)], errors: sc.Map[Int, Failure[CV]]) extends Report[CV]
}
