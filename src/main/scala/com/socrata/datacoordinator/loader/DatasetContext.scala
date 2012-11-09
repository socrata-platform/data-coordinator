package com.socrata.datacoordinator.loader

/** Extracts information about a dataset and from rows within the context of a dataset. */
trait DatasetContext[CT, CV] {
  def hasCopy: Boolean

  def baseName: String

  def userSchema: Map[String, CT]

  def userPrimaryKeyColumn: Option[String]
  def hasUserPrimaryKey: Boolean = userPrimaryKeyColumn.isDefined
  def userPrimaryKey(row: Row[CV]): Option[CV]

  def systemId(row: Row[CV]): Option[Long]
  def systemIdAsValue(row: Row[CV]): Option[CV]

  def systemColumns(row: Row[CV]): Set[String]
  def systemSchema: Map[String, CT]
  def systemIdColumnName: String

  def fullSchema: Map[String, CT]

  def primaryKeyColumn: String = userPrimaryKeyColumn.getOrElse(systemIdColumnName)

  def mergeRows(base: Row[CV], overlay: Row[CV]): Row[CV]
}
