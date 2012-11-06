package com.socrata.datacoordinator.loader

// "schema" does not include system columns, but rows do...
class TestDatasetContext(val baseName: String, val schema: Map[String, TestColumnType], val userPrimaryKeyColumn: Option[String]) extends DatasetContext[TestColumnType, TestColumnValue] {
  userPrimaryKeyColumn.foreach { pkCol =>
    require(schema.contains(pkCol), "PK col defined but does not exist in the schema")
  }

  def hasCopy = sys.error("hasCopy called")

  def hasUserPrimaryKey = userPrimaryKeyColumn.isDefined

  def userPrimaryKey(row: Row[TestColumnValue]) = for {
    userPKColumn <- userPrimaryKeyColumn
    value <- row.get(userPKColumn)
  } yield value

  def systemId(row: Row[TestColumnValue]) =
    row.get(systemIdColumnName).map(_.asInstanceOf[LongValue].value)

  def systemIdAsValue(row: Row[TestColumnValue]) = row.get(systemIdColumnName)

  def systemColumns(row: Row[TestColumnValue]) = row.keySet.filter(_.startsWith(":"))

  def systemIdColumnName = ":id"
}
