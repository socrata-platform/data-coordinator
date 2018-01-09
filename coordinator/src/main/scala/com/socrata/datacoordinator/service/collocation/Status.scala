package com.socrata.datacoordinator.service.collocation

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.codec.JsonEncode

sealed abstract class Status(val name: String) {
  final override def toString = name

  def message: String
}

case object Approved extends Status("approved") {
  override def message: String = "collocation is approved"
}

case object InProgress extends Status("in-progress") {
  override def message: String = "collocation is in progress"
}

case object Completed extends Status("completed") {
  override def message: String = "collocation is complete"
}

case class Rejected(reason: String) extends Status("rejected") {
  override def message: String = s"collocation is rejected because $reason"
}

object Status extends {
  implicit object ordering extends Ordering[Status] {
    private val ordered = Array(Approved, InProgress, Completed)
    override def compare(x: Status, y: Status): Int = {
      (x, y) match {
        case (Rejected(_), Rejected(_)) => 0
        case (Rejected(_), _) => -1
        case (_, Rejected(_)) => 1
        case _ => ordered.indexOf(x) - ordered.indexOf(y)
      }
    }
  }

  implicit object encode extends JsonEncode[Status] {
    override def encode(x: Status): JValue = JString(x.name)
  }

  def combine(x: Status, y: Status): Status = {
    Seq(x, y).min
  }
}
