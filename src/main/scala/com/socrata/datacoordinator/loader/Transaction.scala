package com.socrata.datacoordinator.loader

import scala.{collection => sc}

trait Transaction[CV] {
  def upsert(row: Row[CV])
  def delete(id: CV)
  def lookup(id: CV): Option[Row[CV]]

  def report: Report[CV]

  def commit()
}

trait Report[CV] {
  def inserted: Int
  def updated: Int
  def deleted: Int
  def errors: Int

  def details: sc.Map[Int, JobResult[CV]]
}

sealed abstract class JobResult[+CV]
case class RowCreated[CV](id: CV) extends JobResult[CV]
case class RowUpdated[CV](id: CV) extends JobResult[CV]
case class RowDeleted[CV](id: CV) extends JobResult[CV]
sealed abstract class Failure[+CV] extends JobResult[CV]
case object NullPrimaryKey extends Failure[Nothing]
case class SystemColumnsSet(names: Set[String]) extends Failure[Nothing]
case class NoSuchRowToDelete[CV](id: CV) extends Failure[CV]
case class NoSuchRowToUpdate[CV](id: CV) extends Failure[CV]
case object NoPrimaryKey extends Failure[Nothing]

