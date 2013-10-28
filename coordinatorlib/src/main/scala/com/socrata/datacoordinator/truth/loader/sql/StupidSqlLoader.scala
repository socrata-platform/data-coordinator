package com.socrata.datacoordinator
package truth.loader
package sql

import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.util.{RowIdProvider, RowVersionProvider}
import com.socrata.datacoordinator.id.RowVersion

class StupidSqlLoader[CT, CV](val connection: Connection,
                              val rowPreparer: RowPreparer[CV],
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

  def upsert(job: Int, unpreparedRow: Row[CV]) {
    checkJob(job)
    datasetContext.userPrimaryKeyColumn match {
      case Some(pkCol) =>
        unpreparedRow.get(pkCol) match {
          case Some(id) =>
            findRow(id) match {
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
                  reportWriter.updated(job, IdAndVersion(id, newVersion))
                }
                versionOf(unpreparedRow) match {
                  case None => doUpdate()
                  case Some(Some(v)) if v == oldVersion => doUpdate()
                  case Some(other) => reportWriter.error(job, VersionMismatch(id, Some(oldVersion), other))
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
                    reportWriter.inserted(job, IdAndVersion(id, version))
                  case Some(other) =>
                    reportWriter.error(job, VersionMismatch(id, None, other))
                }
            }
          case None =>
            reportWriter.error(job, NoPrimaryKey)
        }
      case None =>
        unpreparedRow.get(datasetContext.systemIdColumn) match {
          case Some(id) =>
            using(connection.createStatement()) { stmt =>
              findRow(id) match {
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
                    reportWriter.updated(job, IdAndVersion(id, newVersion))
                  }
                  versionOf(unpreparedRow) match {
                    case None => doUpdate()
                    case Some(Some(v)) if v == oldVersion => doUpdate()
                    case Some(other) => reportWriter.error(job, VersionMismatch(id, None, other))
                  }
                case None =>
                  reportWriter.error(job, NoSuchRowToUpdate(id))
              }
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
                reportWriter.inserted(job, IdAndVersion(insertRow(datasetContext.systemIdColumn), typeContext.makeRowVersionFromValue(insertRow(datasetContext.versionColumn))))
              case Some(Some(_)) =>
                reportWriter.error(job, VersionOnNewRow)
            }
        }
    }
  }

  def findRow(id: CV): Option[InspectedRow[CV]] = {
    using(sqlizer.findRows(connection, Iterator.single(id))) { blocks =>
      val sids = blocks.flatten.toSeq
      assert(sids.length < 2)
      sids.headOption
    }
  }

  def findRowId(id: CV): Option[InspectedRowless[CV]] = {
    using(sqlizer.findIdsAndVersions(connection, Iterator.single(id))) { blocks =>
      val sids = blocks.flatten.toSeq
      assert(sids.length < 2)
      sids.headOption
    }
  }

  def delete(job: Int, id: CV, version: Option[Option[RowVersion]]) {
    checkJob(job)
    findRow(id) match {
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
          case Some(other) => reportWriter.error(job, VersionMismatch(id, Some(oldVersion), other))
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
