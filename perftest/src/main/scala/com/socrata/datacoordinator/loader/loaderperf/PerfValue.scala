package com.socrata.datacoordinator.loader.loaderperf

sealed abstract class PerfValue {
  def sqlize: String
}
case class PVId(value: Long) extends PerfValue {
  def sqlize = value.toString
}
case class PVNumber(value: BigDecimal) extends PerfValue {
  def sqlize = value.toString
}
case class PVText(value: String) extends PerfValue {
  def sqlize = "'" + value.replaceAllLiterally("'", "''") + "'"
}
case object PVNull extends PerfValue {
  def sqlize = "NULL"
}
