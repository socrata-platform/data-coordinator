package com.socrata.datacoordinator.loader

/** Extracts information about a dataset and from rows within the context of a dataset. */
trait DatasetContext[CT, CV] {
  def hasCopy: Boolean

  def baseName: String

  def schema: Map[String, CT]

  def hasUserPrimaryKey: Boolean
  def userPrimaryKeyColumn: Option[String]
  def userPrimaryKey(row: Row[CV]): Option[CV]

  def systemId(row: Row[CV]): Option[Long]
  def systemIdAsValue(row: Row[CV]): Option[CV]

  def systemColumns(row: Row[CV]): Set[String]
  def systemIdColumnName: String
}
