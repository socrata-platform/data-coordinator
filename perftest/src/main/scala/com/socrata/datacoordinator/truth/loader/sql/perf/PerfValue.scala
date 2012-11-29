package com.socrata.datacoordinator
package truth.loader
package sql
package perf

import runtime.ScalaRunTime

sealed abstract class PerfValue {
  def sqlize: String
}
case class PVId(value: Long) extends PerfValue {
  def sqlize = value.toString
}
case class PVNumber(value: BigDecimal) extends PerfValue {
  def sqlize = value.toString

  @volatile private var hashComputed = false
  private var hash: Int = _
  override def hashCode = {
    if(!hashComputed) { hash = ScalaRunTime._hashCode(this); hashComputed = true }
    hash
  }
}
case class PVText(value: String) extends PerfValue {
  def sqlize = "'" + value.replaceAllLiterally("'", "''") + "'"
}
case object PVNull extends PerfValue {
  def sqlize = "NULL"
}
