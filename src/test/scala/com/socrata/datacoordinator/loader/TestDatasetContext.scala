package com.socrata.datacoordinator.loader

class TestDatasetContext(val baseName: String, val userSchema: Map[String, TestColumnType], val userPrimaryKeyColumn: Option[String]) extends DatasetContext[TestColumnType, TestColumnValue] {
  userPrimaryKeyColumn.foreach { pkCol =>
    require(userSchema.contains(pkCol), "PK col defined but does not exist in the schema")
  }

  userSchema.keys.foreach { col =>
    require(!col.startsWith(":"), "User schema column starts with :")
  }

  def hasCopy = sys.error("hasCopy called")

  def userPrimaryKey(row: Row[TestColumnValue]) = for {
    userPKColumn <- userPrimaryKeyColumn
    value <- row.get(userPKColumn)
  } yield value

  def systemId(row: Row[TestColumnValue]) =
    row.get(systemIdColumnName).map(_.asInstanceOf[LongValue].value)

  def systemIdAsValue(row: Row[TestColumnValue]) = row.get(systemIdColumnName)

  def systemColumns(row: Row[TestColumnValue]) = row.keySet.filter(_.startsWith(":"))
  val systemSchema = TestDatasetContext.systemSchema

  val fullSchema = userSchema ++ systemSchema

  def systemIdColumnName = ":id"
}

object TestDatasetContext {
  val systemSchema = Map(":id" -> LongColumn)
}
