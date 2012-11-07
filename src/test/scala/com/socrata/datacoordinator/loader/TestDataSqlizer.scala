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
  def logInsert(rowId: TestColumnValue) =
    "INSERT INTO " + logTableName + " (row, action, who) VALUES (" + rowId.sqlize + ",'insert'," + userSqlized + ")"

  def logUpdate(rowId: TestColumnValue) =
    "INSERT INTO " + logTableName + " (row, action, who) VALUES (" + rowId.sqlize + ",'update'," + userSqlized + ")"

  def logDelete(rowId: TestColumnValue) =
    "INSERT INTO " + logTableName + " (row, action, who) VALUES (" + rowId.sqlize + ",'delete'," + userSqlized + ")"

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
}
