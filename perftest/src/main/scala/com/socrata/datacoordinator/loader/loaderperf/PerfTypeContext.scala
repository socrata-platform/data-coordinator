package com.socrata.datacoordinator.loader
package loaderperf

object PerfTypeContext extends TypeContext[PerfValue] {
  def isNull(value: PerfValue) = value eq PVNull

  def makeValueFromSystemId(id: Long) = PVId(id)

  def makeSystemIdFromValue(id: PerfValue) = id match {
    case PVId(x) => x
    case _ => sys.error("Not an id: " + id)
  }

  val nullValue = PVNull
}
