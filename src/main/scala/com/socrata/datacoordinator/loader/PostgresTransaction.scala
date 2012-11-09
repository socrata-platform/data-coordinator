package com.socrata.datacoordinator.loader

import scala.{collection => sc}
import sc.{mutable => scm}
import sc.JavaConverters._

import java.sql.Connection

import com.rojoma.simplearm.util._
import com.socrata.id.numeric.{Unallocatable, IdProvider}
import gnu.trove.set.hash.TLongHashSet
import gnu.trove.map.hash.TLongObjectHashMap

abstract class PostgresTransaction[CT, CV](val connection: Connection,
                                           val typeContext: TypeContext[CV],
                                           val sqlizer: DataSqlizer[CT, CV],
                                           val idProvider: IdProvider with Unallocatable)
  extends Transaction[CV]
{
  require(!connection.getAutoCommit, "Connection must be in non-auto-commit mode")

  val datasetContext = sqlizer.datasetContext

  val initialBatchSize = 200
  val maxBatchSize = 1600

  var totalRows = 0
  val inserted = new java.util.HashMap[Int, CV]
  val elided = new java.util.HashMap[Int, (CV, Int)]
  val updated = new java.util.HashMap[Int, CV]
  val deleted = new java.util.HashMap[Int, CV]
  val errors = new java.util.HashMap[Int, Failure[CV]]

  using(connection.createStatement()) { stmt =>
    stmt.execute(sqlizer.lockTableAgainstWrites(sqlizer.dataTableName))
  }

  def nextJobNum() = {
    val r = totalRows
    totalRows += 1
    r
  }

  def logRowsChanged(ids: TLongHashSet) {
    if(!ids.isEmpty) {
      using(connection.prepareStatement(sqlizer.prepareLogRowChanged)) { stmt =>
        val it = ids.iterator
        while(it.hasNext) {
          stmt.setLong(1, it.next())
          stmt.addBatch()
        }
        val results = stmt.executeBatch()
        assert(results.length == ids.size, "Insert log records added the wrong number of things")
        assert(results.forall(_ == 1), "Insert log records didn't all succeed")
      }
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
    new PostgresTransaction.JobReport(inserted.asScala, updated.asScala, deleted.asScala, elided.asScala, errors.asScala)
  }

  def commit() {
    flush()
    if(inserted != 0 || updated != 0 || deleted != 0) sqlizer.logTransactionComplete()
    connection.commit()
  }
}

class SystemPKPostgresTransaction[CT, CV](_c: Connection, _tc: TypeContext[CV], _s: DataSqlizer[CT, CV], _i: IdProvider with Unallocatable)
  extends PostgresTransaction(_c, _tc, _s, _i)
{
  import PostgresTransaction.SystemIDOps._

  val primaryKey = datasetContext.systemIdColumnName

  val knownToExist = new TLongHashSet()
  val knownNotToExist = new TLongHashSet()
  var batchThreshold = initialBatchSize

  val jobs = new TLongObjectHashMap[Operation[CV]]()

  def forceExisting(id: Long) {
    knownNotToExist.remove(id)
    knownToExist.add(id)
  }

  def forceNonExisting(id: Long) {
    knownNotToExist.add(id)
    knownToExist.remove(id)
  }

  def upsert(row: Row[CV]) {
    val job = nextJobNum()
    row.get(primaryKey) match {
      case Some(systemIdValue) => // update
        if(typeContext.isNull(systemIdValue)) {
          errors.put(job, NullPrimaryKey)
        } else checkNoSystemColumnsExceptId(row) match {
          case None =>
            val systemId = typeContext.makeSystemIdFromValue(systemIdValue)
            if(knownNotToExist.contains(systemId)) {
              errors.put(job, NoSuchRowToUpdate(systemIdValue))
            } else {
              val oldJobNullable = jobs.get(systemId)
              if(oldJobNullable == null) { // first job of this type
                maybeFlush()
                jobs.put(systemId, Update(systemId, row, job))
              } else oldJobNullable match {
                case Insert(insSid, oldRow, oldJob) =>
                  assert(insSid == systemId)
                  jobs.put(systemId, Insert(systemId, datasetContext.mergeRows(oldRow, row), oldJob))
                  elided.put(job, (systemIdValue, oldJob))
                case Update(updSid, oldRow, oldJob) =>
                  assert(updSid == systemId)
                  jobs.put(systemId, Update(systemId, datasetContext.mergeRows(oldRow, row), oldJob))
                  elided.put(job, (systemIdValue, oldJob))
                case _: Delete[_] =>
                  sys.error("Last job for sid was delete, but it is not marked known to not exist?")
              }
              // just because we did an update, it does not mean this row is known to exist
            }
          case Some(error) =>
            errors.put(job, error)
        }
      case None => // insert
        checkNoSystemColumnsExceptId(row) match {
          case None =>
            val systemId = idProvider.allocate()
            val oldJobNullable = jobs.get(systemId)
            if(oldJobNullable == null) {
              maybeFlush()
              jobs.put(systemId, Insert(systemId, row, job))
            } else oldJobNullable match {
              case Delete(_, oldJob) =>
                // hey look at that, we deleted a row that didn't exist yet
                errors.put(oldJob, NoSuchRowToDelete(typeContext.makeValueFromSystemId(systemId)))
                jobs.put(systemId, Insert(systemId, row, job))
              case Update(_, _, oldJob) =>
                // and we updated a row that didn't exist yet, too!
                errors.put(oldJob, NoSuchRowToUpdate(typeContext.makeValueFromSystemId(systemId)))
                jobs.put(systemId, Insert(systemId, row, job))
              case Insert(_, _, _) =>
                sys.error("Allocated the same row ID twice?")
            }
            forceExisting(systemId)
          case Some(error) =>
            errors.put(job, error)
        }
    }
  }

  def delete(id: CV) {
    val job = nextJobNum()
    val systemId = typeContext.makeSystemIdFromValue(id)
    if(knownNotToExist.contains(systemId)) {
      errors.put(job, NoSuchRowToDelete(id))
    } else {
      val oldJobNullable = jobs.get(systemId)
      if(oldJobNullable == null) {
        maybeFlush()
        jobs.put(systemId, Delete(systemId, job))
      } else oldJobNullable match {
        case Update(_, _, oldJob) =>
          // delete-of-update uh?  Well, if this row is known to exist, we can elide the
          // update.  Otherwise we have to flush.
          if(knownToExist.contains(systemId)) {
            elided.put(oldJob, (id, job))
            jobs.put(systemId, Delete(systemId, job))
          } else {
            flush()
            if(knownNotToExist.contains(systemId)) {
              errors.put(job, NoSuchRowToDelete(id))
            } else {
              jobs.put(systemId, Delete(systemId, job))
            }
          }
        case Insert(allocatedSid, _, oldJob) =>
          // deleting a row we just inserted?  Ok.  Let's nuke 'em!
          // Note: not de-allocating sid because we conceptually used it
          elided.put(oldJob, (id, job))
          // and we can skip actually doing this delete too, because we know it'll succeed
          deleted.put(job, id)
          jobs.remove(systemId) // and then we need do nothing with this job
        case Delete(_, _) =>
          sys.error("Got two deletes for the same ID without an intervening update; this should have flagged knownNotToExist.")
          // errors.put(job, NoSuchRowToDelete(typeContext.makeValueFromSystemId(systemId)))
      }
      forceNonExisting(systemId)
    }
  }

  def maybeFlush() {
    if(jobs.size >= batchThreshold) {
      flush()
    }
  }

  def checkNoSystemColumnsExceptId(row: Row[CV]): Option[Failure[CV]] = {
    val systemColumns = datasetContext.systemColumns(row) - primaryKey
    if(systemColumns.isEmpty) None
    else Some(SystemColumnsSet(systemColumns))
  }

  def flush() {
    if(jobs.isEmpty) return

    val deletes = new java.util.ArrayList[Delete[CV]]
    val inserts = new java.util.ArrayList[Insert[CV]]
    val updates = new java.util.ArrayList[Update[CV]]

    val it = jobs.iterator()
    while(it.hasNext) {
      it.advance()
      it.value() match {
        case i@Insert(_,_,_) => inserts.add(i)
        case u@Update(_,_,_) => updates.add(u)
        case d@Delete(_,_) => deletes.add(d)
      }
    }

    jobs.clear()

    val updatedSids = new TLongHashSet
    processDeletes(updatedSids, deletes)
    processUpdates(updatedSids, updates)
    processInserts(updatedSids, inserts)

    logRowsChanged(updatedSids)
  }

  def processDeletes(updatedSids: TLongHashSet, deletes: java.util.ArrayList[Delete[CV]]) {
    if(!deletes.isEmpty) {
      using(connection.prepareStatement(sqlizer.prepareSystemIdDeleteStatement)) { stmt =>
        val it = deletes.iterator()
        while(it.hasNext) {
          val op = it.next()
          sqlizer.prepareSystemIdDelete(stmt, op.id)
        }

        val results = stmt.executeBatch()
        assert(results.length == deletes.size, "Expected " + deletes.size + " results for deletes; got " + results.length)

        var i = 0
        do {
          val op = deletes.get(i)
          val idValue = typeContext.makeValueFromSystemId(op.id)
          if(results(i) == 1) {
            updatedSids.add(op.id)
            deleted.put(op.job, idValue)
          } else if(results(i) == 0) errors.put(op.job, NoSuchRowToDelete(idValue))
          else sys.error("Unexpected result code from delete: " + results(i))
          i += 1
        } while(i != results.length)
      }
    }
  }

  def processUpdates(updatedSids: TLongHashSet, updates: java.util.ArrayList[Update[CV]]) {
    if(!updates.isEmpty) {
      using(connection.createStatement()) { stmt =>
        val it = updates.iterator()
        while(it.hasNext) {
          val op = it.next()
          stmt.addBatch(sqlizer.sqlizeSystemIdUpdate(op.id, op.row))
        }

        val results = stmt.executeBatch()
        assert(results.length == updates.size, "Expected " + updates.size + " results for updates; got " + results.length)

        var i = 0
        do {
          val op = updates.get(i)
          val idValue = typeContext.makeValueFromSystemId(op.id)
          if(results(i) == 1) {
            updatedSids.add(op.id)
            updated.put(op.job, idValue)
          } else if(results(i) == 0) {
            errors.put(op.job, NoSuchRowToUpdate(idValue))
          } else sys.error("Unexpected result code from insert: " + results(i))

          i += 1
        } while(i != results.length)
      }
    }
  }

  def processInserts(updatedSids: TLongHashSet, inserts: java.util.ArrayList[Insert[CV]]) {
    if(!inserts.isEmpty) {
      using(connection.prepareStatement(sqlizer.prepareUserIdInsertStatement)) { stmt =>
        val it = inserts.iterator()
        while(it.hasNext) {
          val op = it.next()
          sqlizer.prepareSystemIdInsert(stmt, op.id, op.row)
        }

        val results = stmt.executeBatch()
        assert(results.length == inserts.size, "Expected " + inserts.size + " results for inserts; got " + results.length)

        var i = 0
        do {
          val op = inserts.get(i)
          val idValue = typeContext.makeValueFromSystemId(op.id)
          if(results(i) == 1) {
            updatedSids.add(op.id)
            inserted.put(op.job, idValue)
          } else sys.error("Unexpected result code from insert: " + results(i))

          i += 1
        } while(i != results.length)
      }
    }
  }

}

class UserPKPostgresTransaction[CT, CV](_c: Connection, _tc: TypeContext[CV], _s: DataSqlizer[CT, CV], _i: IdProvider with Unallocatable)
  extends PostgresTransaction(_c, _tc, _s, _i)
{
  import PostgresTransaction.UserIDOps._

  val primaryKey = datasetContext.userPrimaryKeyColumn.getOrElse(sys.error("Created a UserPKPostgresTranasction but didn't have a user PK"))

  var tryInsertFirst = true

  val knownToExist = datasetContext.makeIdSet()
  val knownNotToExist = datasetContext.makeIdSet()
  val jobs = datasetContext.makeIdMap[OperationLog[CV]]()
  var batchThreshold = initialBatchSize

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
    if(jobs.size >= batchThreshold) {
      partialFlush()
      if(jobs.size >= batchThreshold) { // they were all updates that got changed to inserts
        partialFlush()
        assert(jobs.size == 0, "Partial-flushing the job queue twice didn't clear it completely?")
      }
    }
  }

  def forceExisting(id: CV) {
    knownNotToExist.remove(id)
    knownToExist.add(id)
  }

  def forceNonExisting(id: CV) {
    knownNotToExist.add(id)
    knownToExist.remove(id)
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
            if(record.upsertedRow == null) {
              if(record.hasDeleteJob) {
                assert(knownNotToExist(userId), "Delete job, no upsert job, but not known not to exist?")
              }
              if(knownNotToExist(userId)) {
                record.upsertCase = UpsertInsert
              } else if(knownToExist(userId)) {
                record.upsertCase = UpsertUpdate
              }
              record.upsertedRow = row
              record.upsertJob = job
            } else {
              record.upsertedRow = datasetContext.mergeRows(record.upsertedRow, row)
              elided.put(job, (userId, record.upsertJob))
            }

            forceExisting(userId)
          case Some(error) =>
            errors.put(job, error)
        }
      case None =>
        errors.put(job, NoPrimaryKey)
    }
  }

  def delete(id: CV) {
    val job = nextJobNum()
    if(knownNotToExist(id)) {
      errors.put(job, NoSuchRowToDelete(id))
    } else {
      val record = jobEntry(id)

      if(record.hasUpsertJob) {
        // ok, we'll elide the existing upsert job
        elided.put(record.upsertJob, (id, job))
        record.clearUpsert()

        if(record.hasDeleteJob) {
          // there was a pending deletion before that upsert so we can
          // just call _this_ deletion a success.
          deleted.put(job, id)
        } else {
          // No previous deletion; that upsert may or may not have been
          // an insert, but in either case by the time it got to us, there
          // was something there.  So register the delete job to do and tell
          // the task processor to ignore any failures (which means that the
          // "upsert" was really an insert)
          record.deleteJob = job
          record.forceDeleteSuccess = true
        }
      } else {
        if(record.hasDeleteJob) {
          errors.put(job, NoSuchRowToDelete(id))
        } else {
          record.deleteJob = job
        }
      }

      forceNonExisting(id)
    }
  }

  def checkNoSystemColumns(row: Row[CV]): Option[Failure[CV]] = {
    val systemColumns = datasetContext.systemColumns(row)
    if(systemColumns.isEmpty) None
    else Some(SystemColumnsSet(systemColumns))
  }

  def partialFlush() {
    if(jobs.isEmpty) return

    val deletes = new java.util.ArrayList[OperationLog[CV]]
    val inserts = new java.util.ArrayList[OperationLog[CV]]
    val updates = new java.util.ArrayList[OperationLog[CV]]

    jobs.foreach { (_, op) =>
      if(op.hasDeleteJob) { deletes.add(op) }
      if(op.hasUpsertJob) {
        op.upsertCase match {
          case UpsertUnknown =>
            if(tryInsertFirst) inserts.add(op)
            else updates.add(op)
          case UpsertInsert =>
            inserts.add(op)
          case UpsertUpdate =>
            updates.add(op)
        }
      }
    }

    jobs.clear()

    val sidsForUpdateAndDelete = datasetContext.makeIdMap[Long]()
    val updatedSids = new TLongHashSet
    findSids(sidsForUpdateAndDelete, deletes, inserts, updates)
    processDeletes(updatedSids, sidsForUpdateAndDelete, deletes)
    processInserts(updatedSids, inserts, updates)
    processUpdates(updatedSids, sidsForUpdateAndDelete, updates)

    logRowsChanged(updatedSids)
  }

  def flush() {
    partialFlush()
    partialFlush()
    assert(jobs.isEmpty, "Two partial flushes did not do anything")
  }

  def findSids(target: RowIdMap[CV, Long], ops: java.util.ArrayList[OperationLog[CV]]*) {
    using(connection.createStatement()) { stmt =>
      for(sql <- sqlizer.findSystemIds(ops.iterator.flatMap(_.iterator.asScala).filter { op => op.hasDeleteJob || (op.hasUpsertJob && op.upsertCase != UpsertInsert)}.map(_.id))) {
        for {
          rs <- managed(stmt.executeQuery(sql))
          idPair <- sqlizer.extractIdPairs(rs)
        } target.put(idPair.userId, idPair.systemId)
      }
    }
  }

  def processDeletes(updatedSids: TLongHashSet, sidSource: RowIdMap[CV, Long], deletes: java.util.ArrayList[OperationLog[CV]]) {
    if(!deletes.isEmpty) {
      using(connection.prepareStatement(sqlizer.prepareUserIdDeleteStatement)) { stmt =>
        val it = deletes.iterator()
        while(it.hasNext) {
          val op = it.next()
          assert(op.hasDeleteJob, "No delete job?")
          sqlizer.prepareUserIdDelete(stmt, op.id)
        }

        val results = stmt.executeBatch()
        assert(results.length == deletes.size, "Expected " + deletes.size + " results for deletes; got " + results.length)

        var i = 0
        do {
          val op = deletes.get(i)
          if(results(i) == 1 || op.forceDeleteSuccess) {
            sidSource.get(op.id) match {
              case Some(sid) => updatedSids.add(sid)
              case None if op.forceDeleteSuccess => // ok
              case None => sys.error("Successfully deleted row, but no sid found for it?")
            }
            deleted.put(op.deleteJob, op.id)
          } else if(results(i) == 0) errors.put(op.deleteJob, NoSuchRowToDelete(op.id))
          else sys.error("Unexpected result code from delete: " + results(i))
          i += 1
        } while(i != results.length)
      }
    }
  }

  def processInserts(updatedSids: TLongHashSet, inserts: java.util.ArrayList[OperationLog[CV]], updates: java.util.ArrayList[OperationLog[CV]]) {
    if(!inserts.isEmpty) {
      var failures = 0
      val sids = new Array[Long](inserts.size)
      using(connection.prepareStatement(sqlizer.prepareUserIdInsertStatement)) { stmt =>
        var i = 0
        do {
          val op = inserts.get(i)
          assert(op.hasUpsertJob, "No upsert job?")
          assert(op.upsertCase != UpsertUpdate, "Found an unconditional update while doing inserts?")
          val sid = idProvider.allocate()
          sids(i) = sid
          sqlizer.prepareUserIdInsert(stmt, sid, inserts.get(i).upsertedRow)
          i += 1
        } while(i != inserts.size)

        val results = stmt.executeBatch()
        assert(results.length == inserts.size, "Expected " + inserts.size + " results for inserts; got " + results.length)

        i = 0
        do {
          val op = inserts.get(i)
          if(results(i) == 1) {
            updatedSids.add(sids(i))
            inserted.put(op.upsertJob, op.id)
          } else if(results(i) == 0) {
            idProvider.unallocate(sids(i))
            if(op.upsertCase == UpsertUnknown) {
              failures += 1
              op.upsertCase = UpsertUpdate
              updates.add(op)
            } else {
              sys.error("Unconditional insert failed?")
            }
          } else sys.error("Unexpected result code from insert: " + results(i))

          i += 1
        } while(i != results.length)

        if(failures > (inserts.size >> 1)) tryInsertFirst = !tryInsertFirst
      }
    }
  }

  def processUpdates(updatedSids: TLongHashSet, sidSource: RowIdMap[CV, Long], updates: java.util.ArrayList[OperationLog[CV]]) {
    if(!updates.isEmpty) {
      using(connection.createStatement()) { stmt =>
        var failures = 0
        val it = updates.iterator()
        while(it.hasNext) {
          val op = it.next()
          assert(op.hasUpsertJob, "No upsert job?")
          assert(op.upsertCase != UpsertInsert, "Found an unconditional insert while doing updates?")
          stmt.addBatch(sqlizer.sqlizeUserIdUpdate(op.upsertedRow))
        }

        val results = stmt.executeBatch()
        assert(results.length == updates.size, "Expected " + updates.size + " results for updates; got " + results.length)

        var i = 0
        do {
          val op = updates.get(i)
          if(results(i) == 1) {
            val sid = sidSource.get(op.id).getOrElse(sys.error("Successfully updated row, but no sid found for it?"))
            updatedSids.add(sid)
            updated.put(op.upsertJob, op.id)
          } else if(results(i) == 0) {
            if(op.upsertCase == UpsertUnknown) {
              failures += 1
              op.upsertCase = UpsertInsert
              op.clearDelete()
              jobs.put(op.id, op)
            } else {
              sys.error("Unconditional insert failed?")
            }
          } else sys.error("Unexpected result code from insert: " + results(i))

          i += 1
        } while(i != results.length)

        if(failures > (updates.size >> 1)) tryInsertFirst = !tryInsertFirst
      }
    }
  }
}

object PostgresTransaction {
  def apply[CT, CV](connection: Connection, typeContext: TypeContext[CV], sqlizer: DataSqlizer[CT, CV], idProvider: IdProvider with Unallocatable): PostgresTransaction[CT,CV] = {
    if(sqlizer.datasetContext.hasUserPrimaryKey)
      new UserPKPostgresTransaction(connection, typeContext, sqlizer, idProvider)
    else
      new SystemPKPostgresTransaction(connection, typeContext, sqlizer, idProvider)
  }

  object SystemIDOps {
    sealed abstract class Operation[CV] {
      def id: Long
      def job: Int
    }

    case class Insert[CV](id: Long, row: Row[CV], job: Int) extends Operation[CV]
    case class Update[CV](id: Long, row: Row[CV], job: Int) extends Operation[CV]
    case class Delete[CV](id: Long, job: Int) extends Operation[CV]

    sealed abstract class UpsertType
    case object UpsertInsert extends UpsertType
    case object UpsertUpdate extends UpsertType
  }

  object UserIDOps {
    class OperationLog[CV] {
      var id: CV = _
      var deleteJob: Int = -1
      var forceDeleteSuccess = false

      var upsertJob: Int = -1
      var upsertedRow: Row[CV] = null
      var upsertCase: UpsertType = UpsertUnknown

      def clearDelete() {
        deleteJob = -1
        forceDeleteSuccess = false
      }

      def clearUpsert() {
        upsertJob = -1
        upsertedRow = null
        upsertCase = UpsertUnknown
      }

      def hasDeleteJob = deleteJob != -1
      def hasUpsertJob = upsertJob != -1
    }

    sealed abstract class UpsertType
    case object UpsertUnknown extends UpsertType
    case object UpsertInsert extends UpsertType
    case object UpsertUpdate extends UpsertType
  }

  case class JobReport[CV](inserted: sc.Map[Int, CV], updated: sc.Map[Int, CV], deleted: sc.Map[Int, CV], elided: sc.Map[Int, (CV, Int)], errors: sc.Map[Int, Failure[CV]]) extends Report[CV]
}
