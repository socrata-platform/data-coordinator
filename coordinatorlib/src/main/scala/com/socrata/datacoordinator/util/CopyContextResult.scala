package com.socrata.datacoordinator.util

sealed abstract class CopyContextResult[+T] {
  def map[U](f: T => U): CopyContextResult[U]
  def flatMap[U](f: T => CopyContextResult[U]): CopyContextResult[U]
  def toOption: Option[T]
}

object CopyContextResult {
  case object NoSuchDataset extends CopyContextResult[Nothing] {
    override def map[U](f: Nothing => U): this.type = this
    override def flatMap[U](f: Nothing => CopyContextResult[U]): this.type = this
    override def toOption = None
  }
  case object NoSuchCopy extends CopyContextResult[Nothing] {
    override def map[U](f: Nothing => U): this.type = this
    override def flatMap[U](f: Nothing => CopyContextResult[U]): this.type = this
    override def toOption = None
  }
  case class CopyInfo[+T](value: T) extends CopyContextResult[T] {
    override def map[U](f: T => U) = CopyInfo(f(value))
    override def flatMap[U](f: T => CopyContextResult[U]) = f(value)
    override def toOption = Some(value)
  }
}
