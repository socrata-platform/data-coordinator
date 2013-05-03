package com.socrata.datacoordinator
package truth.loader
package sql

import scala.collection.JavaConverters._

import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.util.{RowDataProvider, Counter}
import com.socrata.datacoordinator.id.RowId

class StupidSqlLoader[CT, CV](val connection: Connection,
                              val rowPreparer: RowPreparer[CV],
                              val sqlizer: DataSqlizer[CT, CV],
                              val dataLogger: DataLogger[CV],
                              val idProvider: RowDataProvider)
  extends Loader[CV]
{
  val datasetContext = sqlizer.datasetContext
  val typeContext = sqlizer.typeContext

  val inserted = new java.util.HashMap[Int, CV]
  val elided = new java.util.HashMap[Int, (CV, Int)]
  val updated = new java.util.HashMap[Int, CV]
  val deleted = new java.util.HashMap[Int, CV]
  val errors = new java.util.HashMap[Int, Failure[CV]]

  var lastJobNum: Int = -1
  def checkJob(job: Int) {
    if(job > lastJobNum) lastJobNum = job
    else throw new IllegalArgumentException("Job numbers must be strictly increasing")
  }

  def upsert(job: Int, unpreparedRow: Row[CV]) {
    checkJob(job)
    datasetContext.userPrimaryKeyColumn match {
      case Some(pkCol) =>
        datasetContext.userPrimaryKey(unpreparedRow) match {
          case Some(id) =>
            findRow(id) match {
              case Some(RowWithId(sid, oldRow)) =>
                rowPreparer.prepareForUpdate(unpreparedRow, oldRow = oldRow) match {
                  case Right(updateRow) =>
                    val updatedCount = using(connection.prepareStatement(sqlizer.prepareSystemIdUpdateStatement)) { stmt =>
                      sqlizer.prepareSystemIdUpdate(stmt, sid, updateRow)
                      stmt.executeUpdate()
                    }
                    assert(updatedCount == 1)
                    dataLogger.update(sid, updateRow)
                    updated.put(job, id)
                  case Left(err) =>
                    errors.put(job, err)
                }
              case None =>
                val sid = idProvider.allocateId()
                rowPreparer.prepareForInsert(unpreparedRow, sid) match {
                  case Right(insertRow) =>
                    val result = sqlizer.insertBatch(connection) { inserter =>
                      inserter.insert(insertRow)
                    }
                    assert(result == 1, "From insert: " + result)
                    dataLogger.insert(sid, insertRow)
                    inserted.put(job, id)
                  case Left(err) =>
                    errors.put(job, err)
                }
            }
          case None =>
            errors.put(job, NoPrimaryKey)
        }
      case None =>
        datasetContext.systemIdAsValue(unpreparedRow) match {
          case Some(id) =>
            using(connection.createStatement()) { stmt =>
              findRow(id) match {
                case Some(RowWithId(sid, oldRow)) =>
                  rowPreparer.prepareForUpdate(unpreparedRow, oldRow = oldRow) match {
                    case Right(updateRow) =>
                      using(connection.prepareStatement(sqlizer.prepareSystemIdUpdateStatement)) { stmt =>
                        sqlizer.prepareSystemIdUpdate(stmt, sid, updateRow)
                        val result = stmt.executeUpdate()
                        assert(result == 1, "From update: " + updated)
                      }
                      updated.put(job, id)
                      dataLogger.update(sid, updateRow)
                    case Left(err) =>
                      errors.put(job, err)
                  }
                case None =>
                  errors.put(job, NoSuchRowToUpdate(id))
              }
            }
          case None =>
            val sid = idProvider.allocateId()
            rowPreparer.prepareForInsert(unpreparedRow, sid) match {
              case Right(insertRow) =>
                val result = sqlizer.insertBatch(connection) { inserter =>
                  inserter.insert(insertRow)
                }
                assert(result == 1, "From insert: " + result)
                dataLogger.insert(sid, insertRow)
                inserted.put(job, typeContext.makeValueFromSystemId(sid))
              case Left(err) =>
                errors.put(job, err)
            }
        }
    }
  }

  def findRow(id: CV): Option[RowWithId[CV]] = {
    using(sqlizer.findRows(connection, Iterator.single(id))) { blocks =>
      val sids = blocks.flatten.toSeq
      assert(sids.length < 2)
      sids.headOption
    }
  }

  def findRowId(id: CV): Option[IdPair[CV]] = {
    using(sqlizer.findSystemIds(connection, Iterator.single(id))) { blocks =>
      val sids = blocks.flatten.toSeq
      assert(sids.length < 2)
      sids.headOption
    }
  }

  def delete(job: Int, id: CV) {
    checkJob(job)
    datasetContext.userPrimaryKeyColumn match {
      case Some(pkCol) =>
        findRowId(id) match {
          case Some(IdPair(sid, _)) =>
            val result = sqlizer.deleteBatch(connection) { deleter =>
              deleter.delete(sid)
            }
            assert(result == 1)
            deleted.put(job, id)
            dataLogger.delete(sid)
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
    SqlLoader.JobReport(inserted.asScala, updated.asScala, deleted.asScala, elided.asScala, errors.asScala)
  }

  def close() {
  }
}
