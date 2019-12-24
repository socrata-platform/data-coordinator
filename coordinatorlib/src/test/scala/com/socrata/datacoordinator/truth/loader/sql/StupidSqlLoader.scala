package com.socrata.datacoordinator.truth.loader.sql

import java.sql.Connection

import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator._
import com.socrata.datacoordinator.id.RowVersion
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.util.{RowIdProvider, RowVersionProvider}

/**
 * For use in testing the real implementation, SqlLoader, of trait Loader
 *
 *    _____________________________________
 *   / StupidSqlLoader should generate the \
 *   | same results as SqlLoader           |
 *   |                                     |
 *   | ... just in a more straightforward  |
 *   \ (stupid) fashion                    /
 *   -------------------------------------
 *          \   ^__^
 *           \  (oo)\_______
 *              (__)\       )\/\
 *                  ||----w |
 *                  ||     ||
 */
class StupidSqlLoader[CT, CV](val connection: Connection,
                              val rowPreparer: RowPreparer[CV],
                              val updateOnly: Boolean,
                              val sqlizer: DataSqlizer[CT, CV],
                              val dataLogger: DataLogger[CV],
                              val idProvider: RowIdProvider,
                              val versionProvider: RowVersionProvider,
                              val reportWriter: ReportWriter[CV])
  extends Loader[CV]
{
  val datasetContext = sqlizer.datasetContext
  val typeContext = sqlizer.typeContext

  var lastJobNum: Int = -1
  def checkJob(job: Int) {
    if(job > lastJobNum) lastJobNum = job
    else throw new IllegalArgumentException("Job numbers must be strictly increasing")
  }

  def versionOf(row: Row[CV]): Option[Option[RowVersion]] =
    row.get(datasetContext.versionColumn) match {
      case Some(v) =>
        if(typeContext.isNull(v)) Some(None)
        else Some(Some(typeContext.makeRowVersionFromValue(v)))
      case None => None
    }

  def upsert(job: Int, unpreparedRow: Row[CV], bySystemId: Boolean) {
    def updateRowBySystemId(id: CV, updateForcedBySystemId: Boolean): Unit = {
      using(connection.createStatement()) { _ =>
        findRow(id, bySystemIdForced = updateForcedBySystemId) match {
          case Some(InspectedRow(_, sid, oldVersion, oldRow)) =>
            def doUpdate() {
              val newVersion = versionProvider.allocate()
              val updateRow = rowPreparer.prepareForUpdate(unpreparedRow, oldRow = oldRow, newVersion = newVersion)
              using(connection.prepareStatement(sqlizer.prepareSystemIdUpdateStatement)) { stmt =>
                sqlizer.prepareSystemIdUpdate(stmt, sid, updateRow)
                val result = stmt.executeUpdate()
                assert(result == 1, "From update: " + result)
              }
              dataLogger.update(sid, Some(oldRow), updateRow)
              reportWriter.updated(job, IdAndVersion(id, newVersion, bySystemIdForced = updateForcedBySystemId))
            }
            versionOf(unpreparedRow) match {
              case None => doUpdate()
              case Some(Some(v)) if v == oldVersion => doUpdate()
              case Some(other) => reportWriter.error(job, VersionMismatch(id, None, other, bySystemIdForced = updateForcedBySystemId))
            }
          case None =>
            reportWriter.error(job, NoSuchRowToUpdate(id, bySystemIdForced = updateForcedBySystemId))
        }
      }
    }

    checkJob(job)
    datasetContext.userPrimaryKeyColumn match {
      case Some(pkCol) =>
        // if there is a user primary key column, first attempt to find the user primary key value
        unpreparedRow.get(pkCol) match {
          case Some(id) =>
            findRow(id, bySystemIdForced = false) match {
              case Some(InspectedRow(_, sid, oldVersion, oldRow)) =>
                def doUpdate() {
                  val newVersion = versionProvider.allocate()
                  val updateRow = rowPreparer.prepareForUpdate(unpreparedRow, oldRow = oldRow, newVersion = newVersion)
                  val updatedCount = using(connection.prepareStatement(sqlizer.prepareSystemIdUpdateStatement)) { stmt =>
                    sqlizer.prepareSystemIdUpdate(stmt, sid, updateRow)
                    stmt.executeUpdate()
                  }
                  assert(updatedCount == 1)
                  dataLogger.update(sid, Some(oldRow), updateRow)
                  reportWriter.updated(job, IdAndVersion(id, newVersion, bySystemIdForced = false))
                }
                versionOf(unpreparedRow) match {
                  case None => doUpdate()
                  case Some(Some(v)) if v == oldVersion => doUpdate()
                  case Some(other) => reportWriter.error(job, VersionMismatch(id, Some(oldVersion), other, bySystemIdForced = false))
                }
              case None =>
                versionOf(unpreparedRow) match {
                  case None | Some(None) =>
                    val sid = idProvider.allocate()
                    val version = versionProvider.allocate()
                    val insertRow = rowPreparer.prepareForInsert(unpreparedRow, sid, version)
                    val (result, ()) = sqlizer.insertBatch(connection) { inserter =>
                      inserter.insert(insertRow)
                    }
                    assert(result == 1, "From insert: " + result)
                    dataLogger.insert(sid, insertRow)
                    reportWriter.inserted(job, IdAndVersion(id, version, bySystemIdForced = false))
                  case Some(other) =>
                    reportWriter.error(job, VersionMismatch(id, None, other, bySystemIdForced = false))
                }
            }
          case None if bySystemId =>
            // here we will update a row with the system id instead of the user primary key if the system id was given
            unpreparedRow.get(datasetContext.systemIdColumn) match {
              case Some(id) => updateRowBySystemId(id, updateForcedBySystemId = true)
              case None => reportWriter.error(job, NoPrimaryKey)
            }
          case None => reportWriter.error(job, NoPrimaryKey)
        }
      case None =>
        // "by_system_id" is considered false here since there is no user primary key column
        unpreparedRow.get(datasetContext.systemIdColumn) match {
          case Some(id) =>
            updateRowBySystemId(id, updateForcedBySystemId = false)
          case None =>
            versionOf(unpreparedRow) match {
              case None | Some(None) =>
                val sid = idProvider.allocate()
                val version = versionProvider.allocate()
                val insertRow = rowPreparer.prepareForInsert(unpreparedRow, sid, version)
                if(updateOnly) reportWriter.error(job, InsertInUpdateOnly(insertRow(datasetContext.systemIdColumn), bySystemIdForced = false))
                else {
                  val (result, ()) = sqlizer.insertBatch(connection) { inserter =>
                    inserter.insert(insertRow)
                  }
                  assert(result == 1, "From insert: " + result)
                  dataLogger.insert(sid, insertRow)
                  reportWriter.inserted(job, IdAndVersion(insertRow(datasetContext.systemIdColumn), typeContext.makeRowVersionFromValue(insertRow(datasetContext.versionColumn)), bySystemIdForced = false))
                }
              case Some(Some(_)) =>
                reportWriter.error(job, VersionOnNewRow)
            }
        }
    }
  }

  def findRow(id: CV, bySystemIdForced: Boolean): Option[InspectedRow[CV]] = {
    using(sqlizer.findRows(connection, bySystemIdForced, Iterator.single(id))) { blocks =>
      val sids = blocks.flatten.toSeq
      assert(sids.length < 2)
      sids.headOption
    }
  }

  def delete(job: Int, id: CV, version: Option[Option[RowVersion]], bySystemId: Boolean) {
    checkJob(job)
    val deleteForcedBySystemId = datasetContext.hasUserPrimaryKey && bySystemId
    findRow(id, deleteForcedBySystemId) match {
      case Some(InspectedRow(_, sid, oldVersion, oldRow)) =>
        def doDelete() {
          val (result, ()) = sqlizer.deleteBatch(connection) { deleter =>
            deleter.delete(sid)
          }
          assert(result == 1)
          reportWriter.deleted(job, id)
          dataLogger.delete(sid, Some(oldRow))
        }
        version match {
          case None => doDelete()
          case Some(Some(v)) if v == oldVersion => doDelete()
          case Some(other) => reportWriter.error(job, VersionMismatch(id, Some(oldVersion), other, bySystemIdForced = deleteForcedBySystemId))
        }
      case None =>
        reportWriter.error(job, NoSuchRowToDelete(id))
    }
  }

  def finish() {
    reportWriter.finished = true
  }

  def close() {
  }
}
