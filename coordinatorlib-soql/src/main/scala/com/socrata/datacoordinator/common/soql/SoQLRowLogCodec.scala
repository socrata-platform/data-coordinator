package com.socrata.datacoordinator.common.soql

import com.socrata.datacoordinator.truth.SimpleRowLogCodec
import com.google.protobuf.{CodedInputStream, CodedOutputStream}
import com.socrata.datacoordinator.id.RowId
import org.joda.time._
import org.joda.time.format.ISODateTimeFormat

object SoQLRowLogCodec extends SimpleRowLogCodec[SoQLValue] {
  def rowDataVersion: Short = 0

  def localDateTimeFormatter = ISODateTimeFormat.dateTime
  def localDateTimeParser = ISODateTimeFormat.localDateOptionalTimeParser
  def localDateFormatter = ISODateTimeFormat.date
  def localDateParser = ISODateTimeFormat.dateElementParser
  def localTimeFormatter = ISODateTimeFormat.time
  def localTimeParser = ISODateTimeFormat.timeElementParser

  // fixme; it'd be much better to do this in a manner simular to how column reps work

  protected def writeValue(target: CodedOutputStream, v: SoQLValue) {
    v match {
      case SoQLIDValue(l) =>
        target.writeRawByte(0)
        target.writeInt64NoTag(l.underlying)
      case SoQLTextValue(s) =>
        target.writeRawByte(1)
        target.writeStringNoTag(s)
      case SoQLNumberValue(bd) =>
        target.writeRawByte(2)
        target.writeStringNoTag(bd.toString)
      case SoQLMoneyValue(bd) =>
        target.writeRawByte(3)
        target.writeStringNoTag(bd.toString)
      case SoQLBooleanValue(b) =>
        target.writeRawByte(4)
        target.writeBoolNoTag(b)
      case SoQLFixedTimestampValue(ts) =>
        target.writeRawByte(5)
        target.writeStringNoTag(ts.getZone.getID)
        target.writeInt64NoTag(ts.getMillis)
      case SoQLLocationValue(lat, lon) =>
        target.writeRawByte(6)
        target.writeDoubleNoTag(lat)
        target.writeDoubleNoTag(lon)
      case SoQLFloatingTimestampValue(ts) =>
        target.writeRawByte(7)
        target.writeStringNoTag(localDateTimeFormatter.print(ts))
      case SoQLDateValue(ts) =>
        target.writeRawByte(8)
        target.writeStringNoTag(localDateFormatter.print(ts))
      case SoQLTimeValue(ts) =>
        target.writeRawByte(9)
        target.writeStringNoTag(localTimeFormatter.print(ts))
      case SoQLNullValue =>
        target.writeRawByte(-1)
    }
  }

  protected def readValue(source: CodedInputStream): SoQLValue =
    source.readRawByte() match {
      case 0 =>
        SoQLIDValue(new RowId(source.readInt64()))
      case 1 =>
        SoQLTextValue(source.readString())
      case 2 =>
        SoQLNumberValue(new java.math.BigDecimal(source.readString()))
      case 3 =>
        SoQLMoneyValue(new java.math.BigDecimal(source.readString()))
      case 4 =>
        SoQLBooleanValue.canonical(source.readBool())
      case 5 =>
        val zone = DateTimeZone.forID(source.readString())
        SoQLFixedTimestampValue(new DateTime(source.readInt64(), zone))
      case 6 =>
        val lat = source.readDouble()
        val lon = source.readDouble()
        SoQLLocationValue(lat, lon)
      case 7 =>
        SoQLFloatingTimestampValue(localDateTimeParser.parseLocalDateTime(source.readString()))
      case 8 =>
        SoQLDateValue(localDateParser.parseLocalDate(source.readString()))
      case 9 =>
        SoQLTimeValue(localTimeParser.parseLocalTime(source.readString()))
      case -1 =>
        SoQLNullValue
    }
}
