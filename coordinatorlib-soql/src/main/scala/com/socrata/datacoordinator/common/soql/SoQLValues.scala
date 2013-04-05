package com.socrata.datacoordinator.common.soql

import com.rojoma.json.util.AutomaticJsonCodecBuilder
import com.rojoma.json.util.JsonKey
import com.socrata.soql.types._
import org.joda.time.{LocalTime, LocalDate, LocalDateTime, DateTime}
import com.socrata.datacoordinator.id.RowId

sealed trait SoQLValueCompanion {
  val typ: SoQLType
}

sealed trait SoQLValue {
  def companion: SoQLValueCompanion
  def typ: SoQLType = companion.typ
}

case object SoQLNullValue extends SoQLValue with SoQLValueCompanion {
  def value = this
  def companion = this
  override val typ = SoQLNull
}

case class SoQLIDValue(value: RowId) extends SoQLValue {
  def companion = SoQLIDValue
  def underlying = value.underlying
}
object SoQLIDValue extends SoQLValueCompanion with (RowId => SoQLIDValue) {
  val typ = SoQLID
}

case class SoQLTextValue(value: String) extends SoQLValue {
  def companion = SoQLTextValue
}
case object SoQLTextValue extends SoQLValueCompanion {
  val typ = SoQLText
}

case class SoQLBooleanValue(value: Boolean) extends SoQLValue {
  def companion = SoQLBooleanValue
}
object SoQLBooleanValue extends SoQLValueCompanion {
  val typ = SoQLBoolean
  val canonicalTrue = SoQLBooleanValue(true)
  val canonicalFalse = SoQLBooleanValue(false)
  def canonical(b: Boolean) = if(b) canonicalTrue else canonicalFalse
}

case class SoQLNumberValue(value: java.math.BigDecimal) extends SoQLValue {
  def companion = SoQLNumberValue
}
object SoQLNumberValue extends SoQLValueCompanion {
  val typ = SoQLNumber
}

case class SoQLMoneyValue(value: java.math.BigDecimal) extends SoQLValue {
  def companion = SoQLMoneyValue
}
object SoQLMoneyValue extends SoQLValueCompanion {
  val typ = SoQLMoney
}

case class SoQLDoubleValue(value: Double) extends SoQLValue {
  def companion = SoQLDoubleValue
}
object SoQLDoubleValue extends SoQLValueCompanion {
  val typ = SoQLDouble
}

case class SoQLFixedTimestampValue(value: DateTime) extends SoQLValue {
  def companion = SoQLFixedTimestampValue
}
object SoQLFixedTimestampValue extends SoQLValueCompanion {
  val typ = SoQLFixedTimestamp
}

case class SoQLFloatingTimestampValue(value: LocalDateTime) extends SoQLValue {
  def companion = SoQLFloatingTimestampValue
}
object SoQLFloatingTimestampValue extends SoQLValueCompanion {
  val typ = SoQLFloatingTimestamp
}

case class SoQLDateValue(value: LocalDate) extends SoQLValue {
  def companion = SoQLDateValue
}
object SoQLDateValue extends SoQLValueCompanion {
  val typ = SoQLDate
}

case class SoQLTimeValue(value: LocalTime) extends SoQLValue {
  def companion = SoQLTimeValue
}
object SoQLTimeValue extends SoQLValueCompanion {
  val typ = SoQLTime
}

case class SoQLLocationValue(@JsonKey("lat") latitude: Double, @JsonKey("lon") longitude: Double) extends SoQLValue {
  def companion = SoQLLocationValue
}

object SoQLLocationValue extends SoQLValueCompanion {
  implicit val jCodec = AutomaticJsonCodecBuilder[SoQLLocationValue]
  val typ = SoQLLocation
}
