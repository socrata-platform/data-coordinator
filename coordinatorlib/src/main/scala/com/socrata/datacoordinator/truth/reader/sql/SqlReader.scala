package com.socrata.datacoordinator
package truth.reader
package sql

import java.sql.{PreparedStatement, Connection}

import com.rojoma.simplearm.util._
import gnu.trove.map.hash.TLongObjectHashMap

import com.socrata.datacoordinator.util.{LeakDetect, CloseableIterator, FastGroupedIterator}
import com.socrata.datacoordinator.truth.sql.{SqlPKableColumnReadRep, SqlColumnReadRep}
import com.socrata.datacoordinator.truth.{DatasetContext, TypeContext}

class SqlReader[CT, CV](connection: Connection,
                        dataTableName: String,
                        datasetContext: DatasetContext[CT, CV],
                        typeContext: TypeContext[CT, CV],
                        repSchemaBuilder: Map[String, CT] => Map[String,SqlColumnReadRep[CT, CV]],
                        val blockSize: Int = 100)
  extends Reader[CV]
{
  val repSchema = repSchemaBuilder(datasetContext.fullSchema)

  def close() {}

  private class SidIterator(columns: Seq[String], ids: Iterator[Long]) extends CloseableIterator[Seq[(Long, Option[Row[CV]])]] {
    val sidRep = repSchema(datasetContext.systemIdColumnName).asInstanceOf[SqlPKableColumnReadRep[CT, CV]]
    val underlying = new FastGroupedIterator(ids, blockSize)
    val selectPrefix = {
      val sb = new StringBuilder("SELECT ")
      sb.append(sidRep.physColumnsForQuery.mkString(","))
      if(!columns.isEmpty) {
        sb.append(",")
        sb.append(columns.flatMap(c => repSchema(c).physColumnsForQuery).mkString(","))
      }
      sb.append(" FROM ").append(dataTableName).append(" WHERE ")
      sb.toString
    }
    var stmt: PreparedStatement = null

    def hasNext = underlying.hasNext

    def next() = {
      val ids = underlying.next()
      ids.zip(lookup(ids))
    }

    def close() {
      if(stmt != null) stmt.close()
    }

    def lookup(block: Seq[Long]): Seq[Option[Row[CV]]] = {
      if(block.length == blockSize)
        lookupFull(block)
      else
        lookupPartial(block)
    }

    def lookupFull(block: Seq[Long]): Seq[Option[Row[CV]]] = {
      if(stmt == null) stmt = connection.prepareStatement(selectPrefix + sidRep.templateForMultiLookup(blockSize))
      finishLookup(stmt, block)
    }

    def lookupPartial(block: Seq[Long]): Seq[Option[Row[CV]]] = {
      using(connection.prepareStatement(selectPrefix + sidRep.templateForMultiLookup(block.length))) { stmt =>
        finishLookup(stmt, block)
      }
    }

    def finishLookup(stmt: PreparedStatement, block: Seq[Long]): Seq[Option[Row[CV]]] = {
      block.foldLeft(1) { (start, id) =>
        sidRep.prepareMultiLookup(stmt, typeContext.makeValueFromSystemId(id), start)
      }
      using(stmt.executeQuery()) { rs =>
        val result = new TLongObjectHashMap[Row[CV]]
        while(rs.next()) {
          val sid = typeContext.makeSystemIdFromValue(sidRep.fromResultSet(rs, 1))
          val row = Map.newBuilder[String, CV]
          var i = 1 + sidRep.physColumnsForQuery.length
          for(c <- columns) {
            val rep = repSchema(c)
            val v = rep.fromResultSet(rs, i)
            i += rep.physColumnsForQuery.length
            row += c -> v
          }
          result.put(sid, row.result())
        }
        block.map { sid =>
          Option(result.get(sid))
        }
      }
    }
  }

  def lookupBySystemId(columns: Iterable[String], ids: Iterator[Long]): CloseableIterator[Seq[(Long, Option[Row[CV]])]] =
    new SidIterator(columns.toSeq, ids) with LeakDetect

  private class UidIterator(columns: Seq[String], ids: Iterator[CV]) extends CloseableIterator[Seq[(CV, Option[Row[CV]])]] {
      val uidRep = repSchema(datasetContext.userPrimaryKeyColumn.getOrElse(sys.error("No user ID defined"))).asInstanceOf[SqlPKableColumnReadRep[CT, CV]]
      val underlying = new FastGroupedIterator(ids, blockSize)
      val selectPrefix = {
        val sb = new StringBuilder("SELECT ")
        sb.append(uidRep.physColumnsForQuery.mkString(","))
        if(!columns.isEmpty) {
          sb.append(",")
          sb.append(columns.flatMap(c => repSchema(c).physColumnsForQuery).mkString(","))
        }
        sb.append(" FROM ").append(dataTableName).append(" WHERE ")
        sb.toString
      }
      var stmt: PreparedStatement = null

      def hasNext = underlying.hasNext

      def next() = {
        val ids = underlying.next()
        ids.zip(lookup(ids))
      }

      def close() {
        if(stmt != null) stmt.close()
      }

      def lookup(block: Seq[CV]): Seq[Option[Row[CV]]] = {
        if(block.length == blockSize)
          lookupFull(block)
        else
          lookupPartial(block)
      }

      def lookupFull(block: Seq[CV]): Seq[Option[Row[CV]]] = {
        if(stmt == null) stmt = connection.prepareStatement(selectPrefix + uidRep.templateForMultiLookup(blockSize))
        finishLookup(stmt, block)
      }

      def lookupPartial(block: Seq[CV]): Seq[Option[Row[CV]]] = {
        using(connection.prepareStatement(selectPrefix + uidRep.templateForMultiLookup(block.length))) { stmt =>
          finishLookup(stmt, block)
        }
      }

      def finishLookup(stmt: PreparedStatement, block: Seq[CV]): Seq[Option[Row[CV]]] = {
        block.foldLeft(1) { (start, id) =>
          uidRep.prepareMultiLookup(stmt, id, start)
        }
        using(stmt.executeQuery()) { rs =>
          val result = datasetContext.makeIdMap[Row[CV]]()
          while(rs.next()) {
            val uid = uidRep.fromResultSet(rs, 1)
            val row = Map.newBuilder[String, CV]
            var i = 1 + uidRep.physColumnsForQuery.length
            for(c <- columns) {
              val rep = repSchema(c)
              val v = rep.fromResultSet(rs, i)
              i += rep.physColumnsForQuery.length
              row += c -> v
            }
            result.put(uid, row.result())
          }
          block.map(result.get)
        }
      }
    }

  def lookupByUserId(columns: Iterable[String], ids: Iterator[CV]): CloseableIterator[Seq[(CV, Option[Row[CV]])]] =
    new UidIterator(columns.toSeq, ids) with LeakDetect
}
