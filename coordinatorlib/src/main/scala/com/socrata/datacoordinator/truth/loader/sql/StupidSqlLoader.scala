package com.socrata.datacoordinator
package truth.loader
package sql

import scala.collection.JavaConverters._

import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.util.{Counter, IdProviderPool}
import com.socrata.datacoordinator.truth.TypeContext

class StupidSqlLoader[CT, CV](val connection: Connection,
                              val rowPreparer: RowPreparer[CV],
                              val sqlizer: DataSqlizer[CT, CV],
                              val dataLogger: DataLogger[CV],
                              val idProviderPool: IdProviderPool)
  extends Loader[CV]
{
  val datasetContext = sqlizer.datasetContext
  val typeContext = sqlizer.typeContext

  val inserted = new java.util.HashMap[Int, CV]
  val elided = new java.util.HashMap[Int, (CV, Int)]
  val updated = new java.util.HashMap[Int, CV]
  val deleted = new java.util.HashMap[Int, CV]
  val errors = new java.util.HashMap[Int, Failure[CV]]

  val nextJobNum = new Counter

  val idProvider = idProviderPool.borrow()

  def upsert(unpreparedRow: Row[CV]) {
    val job = nextJobNum()
    datasetContext.userPrimaryKeyColumn match {
      case Some(pkCol) =>
        datasetContext.userPrimaryKey(unpreparedRow) match {
          case Some(id) =>
            val updateRow = rowPreparer.prepareForUpdate(unpreparedRow)
            val updatedCount = using(connection.createStatement()) { stmt =>
              stmt.executeUpdate(sqlizer.sqlizeUserIdUpdate(updateRow))
            }
            if(updatedCount == 0) {
              val sid = RowId(idProvider.allocate())
              val row = rowPreparer.prepareForInsert(unpreparedRow, sid)
              val result = sqlizer.insertBatch(connection) { inserter =>
                inserter.insert(row)
              }
              assert(result == 1, "From insert: " + result)
              dataLogger.insert(sid, row)
              inserted.put(job, id)
            } else {
              val sid = findSid(id).get
              dataLogger.update(sid, updateRow)
              updated.put(job, id)
            }
          case None =>
            errors.put(job, NoPrimaryKey)
        }
      case None =>
        datasetContext.systemId(unpreparedRow) match {
          case Some(id) =>
            using(connection.createStatement()) { stmt =>
              val updateRow = rowPreparer.prepareForUpdate(unpreparedRow)
              if(stmt.executeUpdate(sqlizer.sqlizeSystemIdUpdate(id, updateRow)) == 1) {
                updated.put(job, typeContext.makeValueFromSystemId(id))
                dataLogger.update(id, updateRow)
              } else
                errors.put(job, NoSuchRowToUpdate(typeContext.makeValueFromSystemId(id)))
            }
          case None =>
            val sid = RowId(idProvider.allocate())
            val row = rowPreparer.prepareForInsert(unpreparedRow, sid)
            val result = sqlizer.insertBatch(connection) { inserter =>
              inserter.insert(row)
            }
            assert(result == 1, "From insert: " + result)
            dataLogger.insert(sid, row)
            inserted.put(job, typeContext.makeValueFromSystemId(sid))
        }
    }
  }

  def findSid(id: CV): Option[RowId] = {
    using(sqlizer.findSystemIds(connection, Iterator.single(id))) { blocks =>
      val sids = blocks.flatten.map(_.systemId).toSeq
      assert(sids.length < 2)
      sids.headOption
    }
  }

  def delete(id: CV) {
    val job = nextJobNum()
    datasetContext.userPrimaryKeyColumn match {
      case Some(pkCol) =>
        using(connection.prepareStatement(sqlizer.prepareUserIdDeleteStatement)) { stmt =>
          val sid = findSid(id)
          sqlizer.prepareUserIdDelete(stmt, id)
          val result = stmt.executeUpdate()
          if(result != 1) {
            assert(!sid.isDefined)
            errors.put(job, NoSuchRowToDelete(id))
          } else {
            assert(sid.isDefined)
            deleted.put(job, id)
            dataLogger.delete(sid.get)
          }
        }
      case None =>
        val sid = typeContext.makeSystemIdFromValue(id)
        using(connection.prepareStatement(sqlizer.prepareSystemIdDeleteStatement)) { stmt =>
          sqlizer.prepareSystemIdDelete(stmt, sid)
          val result = stmt.executeUpdate()
          if(result != 1) {
            errors.put(job, NoSuchRowToDelete(id))
          } else {
            deleted.put(job, id)
            dataLogger.delete(sid)
          }
        }
    }
  }

  def report = {
    SqlLoader.JobReport(inserted.asScala, updated.asScala, deleted.asScala, elided.asScala, errors.asScala)
  }

  def close() {
    idProviderPool.release(idProvider)
  }
}
