package com.socrata.datacoordinator.loader

import java.sql.ResultSet

class TestDataSqlizer(user: String, val datasetContext: DatasetContext[TestColumnType, TestColumnValue]) extends TestSqlizer with DataSqlizer[TestColumnType, TestColumnValue] {
  val dataTableName = datasetContext.baseName + "_data"
  val logTableName = datasetContext.baseName + "_log"

  def mapToPhysical(column: String): String =
    if(datasetContext.systemSchema.contains(column)) {
      column.substring(1)
    } else if(datasetContext.userSchema.contains(column)) {
      "u_" + column
    } else {
      sys.error("unknown column " + column)
    }

  val keys = datasetContext.fullSchema.keys.toSeq
  val columns = keys.map(mapToPhysical)
  val pkCol = mapToPhysical(datasetContext.primaryKeyColumn)

  val userSqlized = StringValue(user).sqlize

  val insertPrefix = "INSERT INTO " + dataTableName + " (" + columns.mkString(",") + ") SELECT "
  val insertMidfix = " WHERE NOT EXISTS (SELECT 1 FROM " + dataTableName + " WHERE " + pkCol + " = "
  val insertSuffix = ")"

  def insert(row: Row[TestColumnValue]) = {
    val pfx = keys.map(row.getOrElse(_, NullValue)).map(_.sqlize).mkString(insertPrefix, ",", insertMidfix)
    pfx + row(datasetContext.primaryKeyColumn).sqlize + insertSuffix
  }

  def update(row: Row[TestColumnValue]) =
    "UPDATE " + dataTableName + " SET " + (row - pkCol).map { case (col, v) => mapToPhysical(col) + " = " + v.sqlize }.mkString(",") + " WHERE " + pkCol + " = " + row(datasetContext.primaryKeyColumn).sqlize

  def delete(id: TestColumnValue) =
    "DELETE FROM " + dataTableName + " WHERE " + pkCol + " = " + id.sqlize

  // txn log has (serial, row id, action, who did the update)
  def logRowChanged(sid: Long, action: String) =
    "INSERT INTO " + logTableName + " (row, action, who) VALUES (" + sid + "," + StringValue(action).sqlize + "," + userSqlized + ")"

  def selectRow(id: TestColumnValue): String =
    "SELECT id," + columns.mkString(",") + " FROM " + dataTableName + " WHERE " + pkCol + " = " + id.sqlize

  def extract(resultSet: ResultSet, logicalColumn: String) = {
    datasetContext.fullSchema(logicalColumn) match {
      case LongColumn =>
        val l = resultSet.getLong(mapToPhysical(logicalColumn))
        if(resultSet.wasNull) NullValue
        else LongValue(l)
      case StringColumn =>
        val s = resultSet.getString(mapToPhysical(logicalColumn))
        if(s == null) NullValue
        else StringValue(s)
    }
  }

  // TODO: it is possible that grouping this differently will be more performant in Postgres
  // (e.g., having too many items in an IN clause might cause a full-table scan) -- we need
  // to test this and if necessary find a good heuristic.
  def findSystemIds(ids: Iterator[TestColumnValue]): Iterator[String] = {
    require(datasetContext.hasUserPrimaryKey, "findSystemIds called without a user primary key")
    if(ids.isEmpty) {
      Iterator.empty
    } else {
      val sql = ids.map(_.sqlize).mkString("SELECT id AS sid, " + pkCol + " AS uid FROM " + dataTableName + " WHERE " + pkCol + " IN (", ",", ")")
      Iterator.single(sql)
    }
  }

  def extractIdPairs(rs: ResultSet) = {
    val typ = datasetContext.userSchema(datasetContext.userPrimaryKeyColumn.getOrElse(sys.error("extractIdPairs called without a user primary key")))
    def loop(): Stream[IdPair[TestColumnValue]] = {
      if(rs.next()) {
        val sid = rs.getLong("sid")
        val uid = typ match {
          case LongColumn =>
            val l = rs.getLong("uid")
            if(rs.wasNull) NullValue
            else LongValue(l)
          case StringColumn =>
            val s = rs.getString("uid")
            if(s == null) NullValue
            else StringValue(s)
        }
        IdPair(sid, uid) #:: loop()
      } else {
        Stream.empty
      }
    }
    loop().iterator
  }
}
