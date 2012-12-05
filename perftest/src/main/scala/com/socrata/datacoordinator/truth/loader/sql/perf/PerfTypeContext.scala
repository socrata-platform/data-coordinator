package com.socrata.datacoordinator
package truth.loader
package sql
package perf

import com.socrata.datacoordinator.truth.TypeContext

object PerfTypeContext extends TypeContext[PerfType, PerfValue] {
  def isNull(value: PerfValue) = value eq PVNull

  def makeValueFromSystemId(id: Long) = PVId(id)

  def makeSystemIdFromValue(id: PerfValue) = id match {
    case PVId(x) => x
    case _ => sys.error("Not an id: " + id)
  }

  val nullValue = PVNull

  def typeFromName(name: String) = name match {
    case "id" => PTId
    case "number" => PTNumber
    case "text" => PTText
  }

  def nameFromType(typ: PerfType) = typ match {
    case PTId => "id"
    case PTNumber => "number"
    case PTText => "text"
  }
}
