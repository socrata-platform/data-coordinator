package com.socrata.datacoordinator
package truth.loader
package sql

import scala.collection.JavaConverters._

import java.sql.Connection

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.util.{Counter, IdProviderPool}
import com.socrata.datacoordinator.truth.TypeContext

class StupidSqlLoader[CT, CV](val connection: Connection,
                              val typeContext: TypeContext[CV],
                              val sqlizer: DataSqlizer[CT, CV],
                              val idProviderPool: IdProviderPool)
  extends Loader[CV]
{
  val datasetContext = sqlizer.datasetContext

  val inserted = new java.util.HashMap[Int, CV]
  val elided = new java.util.HashMap[Int, (CV, Int)]
  val updated = new java.util.HashMap[Int, CV]
  val deleted = new java.util.HashMap[Int, CV]
  val errors = new java.util.HashMap[Int, Failure[CV]]

  val nextJobNum = new Counter

  lazy val txnVersion = for {
    stmt <- managed(connection.createStatement())
    rs <- managed(stmt.executeQuery(sqlizer.findCurrentVersion))
  } yield {
    val hasNext = rs.next()
    assert(hasNext, "next version query didn't return anything?")
    rs.getLong(1) + 1
  }
  val nextSubVersionNum = new Counter(init = 1)
  val rowAuxData = sqlizer.newRowAuxDataAccumulator { auxData =>
    using(connection.prepareStatement(sqlizer.prepareLogRowsChangedStatement)) { stmt =>
      sqlizer.prepareLogRowsChanged(stmt, txnVersion, nextSubVersionNum(), auxData)
      val result = stmt.executeUpdate()
      assert(result == 1, "Inserting a log row... didn't insert a log row?")
    }
  }

  val idProvider = idProviderPool.borrow()

  def upsert(row: Row[CV]) {
    val job = nextJobNum()
    datasetContext.userPrimaryKeyColumn match {
      case Some(pkCol) =>
        datasetContext.userPrimaryKey(row) match {
          case Some(id) =>
            val updatedCount = using(connection.createStatement()) { stmt =>
              stmt.executeUpdate(sqlizer.sqlizeUserIdUpdate(row))
            }
            if(updatedCount == 0) {
              val sid = idProvider.allocate()
              val result = sqlizer.insertBatch(connection) { inserter =>
                inserter.insert(sid, row)
              }
              assert(result == 1, "From insert: " + result)
              rowAuxData.update(sid, row)
              inserted.put(job, id)
            } else {
              val sid = findSid(id).get
              rowAuxData.insert(sid, row)
              updated.put(job, id)
            }
          case None =>
            errors.put(job, NoPrimaryKey)
        }
      case None =>
        datasetContext.systemId(row) match {
          case Some(id) =>
            using(connection.createStatement()) { stmt =>
              if(stmt.executeUpdate(sqlizer.sqlizeSystemIdUpdate(id, row)) == 1) {
                updated.put(job, typeContext.makeValueFromSystemId(id))
                rowAuxData.update(id, row - datasetContext.systemIdColumnName)
              } else
                errors.put(job, NoSuchRowToUpdate(typeContext.makeValueFromSystemId(id)))
            }
          case None =>
            val sid = idProvider.allocate()
            val result = sqlizer.insertBatch(connection) { inserter =>
              inserter.insert(sid, row)
            }
            assert(result == 1, "From insert: " + result)
            rowAuxData.insert(sid, row)
            inserted.put(job, typeContext.makeValueFromSystemId(sid))
        }
    }
  }

  def findSid(id: CV): Option[Long] = {
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
            rowAuxData.delete(sid.get)
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
            rowAuxData.delete(sid)
          }
        }
    }
  }

  def report = {
    rowAuxData.finish()
    SqlLoader.JobReport(txnVersion, inserted.asScala, updated.asScala, deleted.asScala, elided.asScala, errors.asScala)
  }

  def close() {
    idProviderPool.release(idProvider)
  }
}
