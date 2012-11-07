package com.socrata.datacoordinator.loader

import scala.{collection => sc}
import sc.JavaConverters._

import java.sql.Connection

import com.rojoma.simplearm.util._
import com.socrata.id.numeric.{Unallocatable, IdProvider}

class PostgresTransaction[CT, CV](connection: Connection, typeContext: TypeContext[CV], sqlizer: DataSqlizer[CT, CV], idProvider: IdProvider with Unallocatable) extends Transaction[CV] {
  import PostgresTransaction._

  require(!connection.getAutoCommit, "Connection must be in non-auto-commit mode")

  val datasetContext = sqlizer.datasetContext
  private val idObserver = typeContext.makeIdObserver()

  var tryInsertFirst = true

  private val initialBatchSize = 200
  private val maxBatchSize = 1600
  private var batch = new Array[Operation[CV]](initialBatchSize)
  private var batchPtr = 0

  private var totalRows = 0
  private val jobResults = new java.util.TreeMap[Int, JobResult[CV]]
  private var inserted = 0
  private var updated = 0
  private var deleted = 0

  using(connection.createStatement()) { stmt =>
    stmt.execute(sqlizer.lockTableAgainstWrites())
  }

  def nextJobNum() = {
    totalRows += 1
    totalRows
  }

  def upsert(row: Row[CV]) {
    if(datasetContext.hasUserPrimaryKey) {
      upsertUserPK(row)
    } else {
      upsertSystemPK(row)
    }
  }

  def delete(id: CV) {
    val job = nextJobNum()
    if(typeContext.isNull(id)) {
      jobResults.put(job, NullPrimaryKey)
    } else {
      if(idObserver.observed(id)) {
        // deleting a row we've seen; flush to the DB in case the previous job retries
        partialFlush()
      }
      addJob(Delete(job, id))
    }
  }

  def upsertUserPK(row: Row[CV]) {
    val job = nextJobNum()
    datasetContext.userPrimaryKey(row) match {
      case Some(userId) =>
        checkNoMetadata(row, idAllowed = false) match {
          case None =>
            if(typeContext.isNull(userId)) {
              jobResults.put(job, NullPrimaryKey)
            } else {
              idObserver.observe(userId)
              if(tryInsertFirst) {
                val systemId = idProvider.allocate()
                addJob(Insert(job, systemId, row))
              } else {
                addJob(Update(job, row))
              }
            }
          case Some(error) =>
            jobResults.put(job, error)
        }
      case None =>
        jobResults.put(job, NoPrimaryKey)
    }
  }

  def upsertSystemPK(row: Row[CV]) {
    val job = nextJobNum()
    checkNoMetadata(row, idAllowed = true) match {
      case None =>
        datasetContext.systemIdAsValue(row) match {
          case Some(systemId) =>
            if(typeContext.isNull(systemId)) {
              jobResults.put(job, NullPrimaryKey)
            } else {
              idObserver.observe(systemId)
              addJob(Update(job, row))
            }
          case None =>
            val systemId = idProvider.allocate()
            idObserver.observe(typeContext.makeSystemIdValue(systemId))
            addJob(Insert(job, systemId, row))
        }
      case Some(error) =>
        jobResults.put(job, error)
    }
  }

  def addJob(op: Operation[CV]) {
    batch(batchPtr) = op
    batchPtr += 1
    if(batchPtr == batch.length) partialFlush()
  }

  def addRetry(op: Operation[CV]) {
    op.secondTry = true
    batch(batchPtr) = op
    batchPtr += 1
  }

  def growBatch() {
    if(batch.length < maxBatchSize) {
      val newBatch = new Array[Operation[CV]](batch.length << 1)
      System.arraycopy(batch, 0, newBatch, 0, batchPtr)
      batch = newBatch
    }
  }

  def checkNoMetadata(row: Row[CV], idAllowed: Boolean): Option[Failure[CV]] = {
    var systemColumns = datasetContext.systemColumns(row)
    if(idAllowed) systemColumns -= datasetContext.systemIdColumnName
    if(systemColumns.isEmpty) None
    else Some(SystemColumnsSet(systemColumns))
  }

  def partialFlush() {
    if(batchPtr == 0) return

    val startingJobCount = batchPtr

    def doFlush(){
      using(connection.createStatement()) { stmt =>
        idObserver.clear()

        var src = 0
        do {
          val sql = batch(src) match {
            case Insert(_, sid, row) =>
              sqlizer.insert(row + (datasetContext.systemIdColumnName -> typeContext.makeSystemIdValue(sid)))
            case Update(_, row) =>
              sqlizer.update(row)
            case Delete(_, id) =>
              sqlizer.delete(id)
          }
          stmt.addBatch(sql)
          src += 1
        } while(src < batchPtr)

        val result = stmt.executeBatch()
        assert(result.length == batchPtr)

        // This look temporarily uses the observer if the last run
        // was in update-before-insert mode, in order to make retrying
        // do only a single insert for a given userPK.

        // First we need to figure out what we'll do if the executeBatch
        // call failed to say "yep, one row updated" for a given query.
        // This will vary based on whether or not there's a user-defined
        // priamry key.
        val processFailed: Operation[CV] => Unit =
          if(datasetContext.hasUserPrimaryKey) {
            case ins@Insert(job, sid, row) =>
              if(ins.secondTry) sys.error("Update failed, then insert failed?")
              idProvider.unallocate(sid)
              addRetry(Update(job, row))
            case up@Update(job, row) =>
              if(up.secondTry) sys.error("Update failed on second go?")
              val rowID = datasetContext.userPrimaryKey(row).getOrElse(sys.error("Had a user ID before?"))
              if(idObserver.observed(rowID)) {
                addRetry(up)
              } else {
                idObserver.observe(rowID)
                val sid = idProvider.allocate()
                addRetry(Insert(job, sid, row))
              }
            case Delete(job, id) =>
              jobResults.put(job, NoSuchRowToDelete(id))
          } else {
            case Update(job, row) =>
              jobResults.put(job, NoSuchRowToUpdate(datasetContext.systemIdAsValue(row).getOrElse(sys.error("Had a system ID before?"))))
            case Delete(job, id) =>
              jobResults.put(job, NoSuchRowToDelete(id))
            case Insert(_, _, _) =>
              sys.error("Insert failed when using system identifier!")
          }

        batchPtr = 0
        src = 0
        var logsToRun = 0
        do { // through this loop, batchPtr <= src
          val op = batch(src)
          batch(src) = null

          val rowsUpdated = result(src)
          if(rowsUpdated == 1) {
            val logStmt = op match {
              case Insert(job, sid, row) =>
                inserted += 1
                val pk = datasetContext.userPrimaryKey(row).getOrElse(typeContext.makeSystemIdValue(sid))
                jobResults.put(job, RowCreated(pk))
                sqlizer.logInsert(pk)
              case Update(job, row) =>
                updated += 1
                val pk = datasetContext.userPrimaryKey(row).orElse(datasetContext.systemIdAsValue(row)).getOrElse(sys.error("Had a system ID before?"))
                jobResults.put(job, RowUpdated(pk))
                sqlizer.logUpdate(pk)
              case Delete(job, id) =>
                deleted += 1
                jobResults.put(job, RowDeleted(id))
                sqlizer.logDelete(id)
            }
            stmt.addBatch(logStmt)
            logsToRun += 1
          } else {
            assert(rowsUpdated == 0, "A statement affected more than one row?")
            processFailed(op)
          }

          src += 1
        } while(src < result.length)

        // at this point "batch" contains the set of operations that need to be retried,
        // packed to the front of the array (and the rest of it has been nulled out), and
        // the stmt has been filled up with a bunch of log additions for all the others.
        // Also we can re-clear the observer because everything that is added should NOT
        // cause a flush if a delete call comes along even if it's for the same row
        // (because the pending inserts/updates should all always succeed).
        idObserver.clear()

        if(logsToRun != 0) {
          val result = stmt.executeBatch()
          assert(result.length == logsToRun, "Didn't run all the logs?")
          assert(result.forall(_ == 1), "Log didn't do a bunch of single inserts?")
        }
      }
    }

    doFlush()

    if(batchPtr != 0 && batchPtr > (batch.length >> 1)) {
      // more than half the queries were guessed wrong; switch our guess for the
      // next go-round.
      tryInsertFirst = !tryInsertFirst
    } else if(startingJobCount == batch.length) {
      // we guessed at least mostly right
      growBatch()
    }

    if(batchPtr == batch.length) {
      // ack, EVERY SINGLE OPERATION retried and there's no room for more!
      doFlush()
      assert(batchPtr == 0, "Flushing an all-retry operation list didn't clear the list?")
      // ok, so we know that
      //    (a) every job we were asked to do was the same (because we retried them all)
      //    (b) startingJobCount == batch.length (because we had retries for that many!)
      //    (c) we've flipped tryInsertFirst (because "all the jobs" is more than "half the jobs")
      // ok, so we'll assume that the next batch will be a good one, and a uniform one.
      // Therefore we'll increase to the next batch size.
      growBatch()
    }
  }

  def flush() {
    partialFlush()
    partialFlush()
    assert(batchPtr == 0, "Flushing an operation list twice didn't clear the list?")
  }

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

    val errors = totalRows - inserted - updated - deleted
    new JobReport(inserted, updated, deleted, errors, jobResults.asScala)
  }

  def commit() {
    flush()
    if(inserted != 0 || updated != 0 || deleted != 0) sqlizer.logTransactionComplete()
    connection.commit()
  }
}

object PostgresTransaction {
  sealed abstract class Operation[CV] {
    def jobNum: Int
    var secondTry = false
  }
  case class Insert[CV](jobNum: Int, systemId: Long, row: Row[CV]) extends Operation[CV]
  case class Update[CV](jobNum: Int, row: Row[CV]) extends Operation[CV]
  case class Delete[CV](jobNum: Int, id: CV) extends Operation[CV]

  case class JobReport[CV](inserted: Int, updated: Int, deleted: Int, errors: Int, details: sc.Map[Int, JobResult[CV]]) extends Report[CV]
}
