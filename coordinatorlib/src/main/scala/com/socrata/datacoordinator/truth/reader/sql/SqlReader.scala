package com.socrata.datacoordinator
package truth.reader
package sql

import java.sql.{PreparedStatement, Connection}

import com.rojoma.simplearm.util._
import gnu.trove.map.hash.TLongObjectHashMap

import com.socrata.datacoordinator.util.{CloseableIterator, FastGroupedIterator}
import com.socrata.datacoordinator.truth.sql.{SqlPKableColumnRep, SqlColumnRep}
import com.socrata.datacoordinator.truth.{DatasetContext, TypeContext}

class SqlReader[CT, CV](connection: Connection,
                        datasetContext: DatasetContext[CT, CV],
                        typeContext: TypeContext[CV],
                        repSchemaBuilder: Map[String, CT] => Map[String,SqlColumnRep[CT, CV]])
  extends Reader[CV]
{
  val repSchema = repSchemaBuilder(datasetContext.fullSchema)

  val blockSize = 100

  def lookupBySystemId(columnsRaw: Iterable[String], ids: Iterable[Long]) = new CloseableIterator[Seq[Row[CV]]] {
    val columns = columnsRaw.toSeq
    val sidRep = repSchema(datasetContext.systemIdColumnName).asInstanceOf[SqlPKableColumnRep[CT, CV]]
    val underlying = new FastGroupedIterator(ids.iterator, blockSize)
    val selectPrefix = {
      val sb = new StringBuilder("SELECT ")
      sb.append(sidRep.physColumnsForQuery.mkString(","))
      if(!columns.isEmpty) {
        sb.append(",")
        sb.append(columns.flatMap(c => repSchema(c).physColumnsForQuery).mkString(","))
      }
      sb.append(" WHERE ")
      sb.toString
    }
    var stmt: PreparedStatement = null

    def hasNext = underlying.hasNext

    def next() = lookup(underlying.next())

    def close() {
      if(stmt != null) stmt.close()
    }

    def lookup(block: Seq[Long]): Seq[Row[CV]] = {
      if(block.length == blockSize)
        lookupFull(block)
      else
        lookupPartial(block)
    }

    def lookupFull(block: Seq[Long]): Seq[Row[CV]] = {
      if(stmt == null) stmt = connection.prepareStatement(selectPrefix + sidRep.templateForMultiLookup(blockSize))
      finishLookup(stmt, block)
    }

    def lookupPartial(block: Seq[Long]): Seq[Row[CV]] = {
      using(connection.prepareStatement(selectPrefix + sidRep.templateForMultiLookup(block.length))) { stmt =>
        finishLookup(stmt, block)
      }
    }

    def finishLookup(stmt: PreparedStatement, block: Seq[Long]): Seq[Row[CV]] = {
      block.foldLeft(1) { (start, id) =>
        sidRep.prepareMultiLookup(stmt, typeContext.makeValueFromSystemId(id), start)
      }
      using(stmt.executeQuery()) { rs =>
        val result = new TLongObjectHashMap[Row[CV]]
        while(rs.next()) {
          val sid = typeContext.makeSystemIdFromValue(sidRep.fromResultSet(rs, 1))
          val row = Map.newBuilder[String, CV]
          var i = 1 + sidRep.physColumnsForInsert.length
          for(c <- columns) {
            val rep = repSchema(c)
            val v = rep.fromResultSet(rs, i)
            i += rep.physColumnsForQuery.length
            row += c -> v
          }
          result.put(sid, row.result())
        }
        block.flatMap { sid =>
          Option(result.get(sid))
        }
      }
    }
  }

  def lookupByUserId(columnsRaw: Iterable[String], ids: Iterable[CV]) = new CloseableIterator[Seq[Row[CV]]] {
    val columns = columnsRaw.toSeq
    val uidRep = repSchema(datasetContext.userPrimaryKeyColumn.getOrElse(sys.error("No user ID defined"))).asInstanceOf[SqlPKableColumnRep[CT, CV]]
    val underlying = new FastGroupedIterator(ids.iterator, blockSize)
    val selectPrefix = {
      val sb = new StringBuilder("SELECT ")
      sb.append(uidRep.physColumnsForQuery.mkString(","))
      if(!columns.isEmpty) {
        sb.append(",")
        sb.append(columns.flatMap(c => repSchema(c).physColumnsForQuery).mkString(","))
      }
      sb.append(" WHERE ")
      sb.toString
    }
    var stmt: PreparedStatement = null

    def hasNext = underlying.hasNext

    def next() = lookup(underlying.next())

    def close() {
      if(stmt != null) stmt.close()
    }

    def lookup(block: Seq[CV]): Seq[Row[CV]] = {
      if(block.length == blockSize)
        lookupFull(block)
      else
        lookupPartial(block)
    }

    def lookupFull(block: Seq[CV]): Seq[Row[CV]] = {
      if(stmt == null) stmt = connection.prepareStatement(selectPrefix + uidRep.templateForMultiLookup(blockSize))
      finishLookup(stmt, block)
    }

    def lookupPartial(block: Seq[CV]): Seq[Row[CV]] = {
      using(connection.prepareStatement(selectPrefix + uidRep.templateForMultiLookup(block.length))) { stmt =>
        finishLookup(stmt, block)
      }
    }

    def finishLookup(stmt: PreparedStatement, block: Seq[CV]): Seq[Row[CV]] = {
      block.foldLeft(1) { (start, id) =>
        uidRep.prepareMultiLookup(stmt, id, start)
      }
      using(stmt.executeQuery()) { rs =>
        val result = datasetContext.makeIdMap[Row[CV]]()
        while(rs.next()) {
          val uid = uidRep.fromResultSet(rs, 1)
          val row = Map.newBuilder[String, CV]
          var i = 1 + uidRep.physColumnsForInsert.length
          for(c <- columns) {
            val rep = repSchema(c)
            val v = rep.fromResultSet(rs, i)
            i += rep.physColumnsForQuery.length
            row += c -> v
          }
          result.put(uid, row.result())
        }
        block.flatMap(result.get)
      }
    }
  }
}
