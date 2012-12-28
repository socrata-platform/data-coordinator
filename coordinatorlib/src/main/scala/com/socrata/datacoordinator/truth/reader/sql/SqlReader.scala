package com.socrata.datacoordinator
package truth.reader
package sql

import java.sql.{PreparedStatement, Connection}

import com.rojoma.simplearm.util._

import com.socrata.datacoordinator.util.{LeakDetect, CloseableIterator, FastGroupedIterator}
import com.socrata.datacoordinator.util.collection.{MutableRowIdMap, ColumnIdMap, MutableColumnIdMap}
import com.socrata.datacoordinator.truth.sql.{SqlPKableColumnReadRep, SqlColumnReadRep}
import com.socrata.datacoordinator.truth.{DatasetContext, TypeContext}
import com.socrata.datacoordinator.id.{RowId, ColumnId}

class SqlReader[CT, CV](connection: Connection,
                        dataTableName: String,
                        datasetContext: DatasetContext[CT, CV],
                        typeContext: TypeContext[CT, CV],
                        repSchemaBuilder: ColumnIdMap[CT] => ColumnIdMap[SqlColumnReadRep[CT, CV]],
                        val blockSize: Int = 100)
  extends Reader[CV]
{
  val repSchema = repSchemaBuilder(datasetContext.fullSchema)

  def close() {}

  private class SidIterator(columns: Seq[ColumnId], ids: Iterator[RowId]) extends CloseableIterator[Seq[(RowId, Option[Row[CV]])]] {
    val sidRep = repSchema(datasetContext.systemIdColumn).asInstanceOf[SqlPKableColumnReadRep[CT, CV]]
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

    def lookup(block: Seq[RowId]): Seq[Option[Row[CV]]] = {
      if(block.length == blockSize)
        lookupFull(block)
      else
        lookupPartial(block)
    }

    def lookupFull(block: Seq[RowId]): Seq[Option[Row[CV]]] = {
      if(stmt == null) stmt = connection.prepareStatement(selectPrefix + sidRep.templateForMultiLookup(blockSize))
      finishLookup(stmt, block)
    }

    def lookupPartial(block: Seq[RowId]): Seq[Option[Row[CV]]] = {
      using(connection.prepareStatement(selectPrefix + sidRep.templateForMultiLookup(block.length))) { stmt =>
        finishLookup(stmt, block)
      }
    }

    def finishLookup(stmt: PreparedStatement, block: Seq[RowId]): Seq[Option[Row[CV]]] = {
      block.foldLeft(1) { (start, id) =>
        sidRep.prepareMultiLookup(stmt, typeContext.makeValueFromSystemId(id), start)
      }
      using(stmt.executeQuery()) { rs =>
        val result = new MutableRowIdMap[Row[CV]]
        while(rs.next()) {
          val sid = typeContext.makeSystemIdFromValue(sidRep.fromResultSet(rs, 1))
          val row = new MutableColumnIdMap[CV]
          var i = 1 + sidRep.physColumnsForQuery.length
          for(c <- columns) {
            val rep = repSchema(c)
            val v = rep.fromResultSet(rs, i)
            i += rep.physColumnsForQuery.length
            row(c) = v
          }
          result(sid) = row.freeze()
        }
        block.map { sid =>
          result.get(sid)
        }
      }
    }
  }

  def lookupBySystemId(columns: Iterable[ColumnId], ids: Iterator[RowId]): CloseableIterator[Seq[(RowId, Option[Row[CV]])]] =
    new SidIterator(columns.toSeq, ids) with LeakDetect

  private class UidIterator(columns: Seq[ColumnId], ids: Iterator[CV]) extends CloseableIterator[Seq[(CV, Option[Row[CV]])]] {
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
            val row = new MutableColumnIdMap[CV]
            var i = 1 + uidRep.physColumnsForQuery.length
            for(c <- columns) {
              val rep = repSchema(c)
              val v = rep.fromResultSet(rs, i)
              i += rep.physColumnsForQuery.length
              row += c -> v
            }
            result.put(uid, row.freeze())
          }
          block.map(result.get)
        }
      }
    }

  def lookupByUserId(columns: Iterable[ColumnId], ids: Iterator[CV]): CloseableIterator[Seq[(CV, Option[Row[CV]])]] =
    new UidIterator(columns.toSeq, ids) with LeakDetect
}
