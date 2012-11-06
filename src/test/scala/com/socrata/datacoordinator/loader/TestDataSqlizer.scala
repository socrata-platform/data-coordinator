package com.socrata.datacoordinator.loader

class TestDataSqlizer(user: String, datasetContext: DatasetContext[TestColumnType, TestColumnValue]) extends TestSqlizer with DataSqlizer[TestColumnValue] {
  val dataTableName = datasetContext.baseName + "_data"
  val logTableName = datasetContext.baseName + "_log"

  def mapToPhysical(column: String): String = "u_" + column

  val keys = datasetContext.schema.keys.toSeq
  val columns = keys.map(mapToPhysical)

  val userSqlized = StringValue(user).sqlize

  def insertPrefix(id: Long)= "INSERT INTO " + dataTableName + " (id, " + columns.mkString(",") + ") SELECT " + id + ","
  def insertSuffix(id: Long) = " WHERE NOT EXISTS (SELECT 1 FROM " + dataTableName + " WHERE id = " + id + ")"
  def insertSuffix(userIdCol: String, id: TestColumnValue) = " WHERE NOT EXISTS (SELECT 1 FROM " + dataTableName + " WHERE " + mapToPhysical(userIdCol) + " = " + id.sqlize + ")"

  def insert(id: Long, row: Row[TestColumnValue]) = {
    val pfx = insertPrefix(id) + keys.map(row.getOrElse(_, NullValue)).map(_.sqlize).mkString(",")
    datasetContext.userPrimaryKeyColumn match {
      case Some(col) =>
        pfx + insertSuffix(col, row(col))
      case None =>
        pfx + insertSuffix(id)
    }
  }

  def update(row: Row[TestColumnValue]) =
    datasetContext.userPrimaryKeyColumn match {
      case Some(pkCol) =>
        "UPDATE " + dataTableName + " SET " + (row - pkCol).map { case (col, v) => mapToPhysical(col) + " = " + v.sqlize }.mkString(",") + " WHERE " + mapToPhysical(pkCol) + " = " + row(pkCol).sqlize
      case None =>
        "UPDATE " + dataTableName + " SET " + (row - datasetContext.systemIdColumnName).map { case (col, v) => mapToPhysical(col) + " = " + v.sqlize }.mkString(",") + " WHERE id = " + row(":id").sqlize
    }

  def delete(id: TestColumnValue) =
    datasetContext.userPrimaryKeyColumn match {
      case Some(pkCol) =>
        "DELETE FROM " + dataTableName + " WHERE " + pkCol + " = " + id.sqlize
      case None =>
        "DELETE FROM " + dataTableName + " WHERE id = " + id.sqlize
    }

  // txn log has (serial, row id, action, who did the update)
  def logInsert(rowId: TestColumnValue) = {
    "INSERT INTO " + logTableName + " (row, action, who) VALUES (" + rowId.sqlize + ",'insert'," + userSqlized + ")"
  }

  def logUpdate(rowId: TestColumnValue) = {
    "INSERT INTO " + logTableName + " (row, action, who) VALUES (" + rowId.sqlize + ",'update'," + userSqlized + ")"
  }

  def logDelete(rowId: TestColumnValue) = {
    "INSERT INTO " + logTableName + " (row, action, who) VALUES (" + rowId.sqlize + ",'delete'," + userSqlized + ")"
  }

  def lookup(id: TestColumnValue) = null
}
