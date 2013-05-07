package com.socrata.datacoordinator
package truth.loader
package sql

import scala.collection.JavaConverters._

import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.util.{RowIdProvider, RowVersionProvider}
import com.socrata.datacoordinator.id.{RowVersion, RowId}

class StupidSqlLoader[CT, CV](val connection: Connection,
                              val rowPreparer: RowPreparer[CV],
                              val sqlizer: DataSqlizer[CT, CV],
                              val dataLogger: DataLogger[CV],
                              val idProvider: RowIdProvider,
                              val versionProvider: RowVersionProvider)
  extends Loader[CV]
{
  val datasetContext = sqlizer.datasetContext
  val typeContext = sqlizer.typeContext

  val inserted = new java.util.HashMap[Int, IdAndVersion[CV]]
  val updated = new java.util.HashMap[Int, IdAndVersion[CV]]
  val deleted = new java.util.HashMap[Int, CV]
  val errors = new java.util.HashMap[Int, Failure[CV]]

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
                  dataLogger.update(sid, updateRow)
                  updated.put(job, IdAndVersion(id, newVersion))
                }
                versionOf(unpreparedRow) match {
                  case None => doUpdate()
                  case Some(Some(v)) if v == oldVersion => doUpdate()
                  case Some(other) => errors.put(job, VersionMismatch(id, Some(oldVersion), other))
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
                    inserted.put(job, IdAndVersion(id, version))
                  case Some(other) =>
                    errors.put(job, VersionMismatch(id, None, other))
                }
            }
          case None =>
            errors.put(job, NoPrimaryKey)
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
                      assert(result == 1, "From update: " + updated)
                    }
                    dataLogger.update(sid, updateRow)
                    updated.put(job, IdAndVersion(id, newVersion))
                  }
                  versionOf(unpreparedRow) match {
                    case None => doUpdate()
                    case Some(Some(v)) if v == oldVersion => doUpdate()
                    case Some(other) => errors.put(job, VersionMismatch(id, None, other))
                  }
                case None =>
                  errors.put(job, NoSuchRowToUpdate(id))
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
                inserted.put(job, IdAndVersion(insertRow(datasetContext.systemIdColumn), typeContext.makeRowVersionFromValue(insertRow(datasetContext.versionColumn))))
              case Some(Some(_)) =>
                errors.put(job, VersionOnNewRow)
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
    datasetContext.userPrimaryKeyColumn match {
      case Some(pkCol) =>
        findRowId(id) match {
          case Some(InspectedRowless(_, sid, oldVersion)) =>
            def doDelete() {
              val (result, ()) = sqlizer.deleteBatch(connection) { deleter =>
                deleter.delete(sid)
              }
              assert(result == 1)
              deleted.put(job, id)
              dataLogger.delete(sid)
            }
            version match {
              case None => doDelete()
              case Some(Some(v)) if v == oldVersion => doDelete()
              case Some(other) => errors.put(job, VersionMismatch(id, Some(oldVersion), other))
            }
          case None =>
            errors.put(job, NoSuchRowToDelete(id))
        }
      case None =>
        val sid = typeContext.makeSystemIdFromValue(id)
        val result = sqlizer.deleteBatch(connection) { deleter =>
          deleter.delete(sid)
        }
        if(result != 1) {
          errors.put(job, NoSuchRowToDelete(id))
        } else {
          deleted.put(job, id)
          dataLogger.delete(sid)
        }
    }
  }

  def report = {
    SqlLoader.JobReport(inserted.asScala, updated.asScala, deleted.asScala, errors.asScala)
  }

  def close() {
  }
}
