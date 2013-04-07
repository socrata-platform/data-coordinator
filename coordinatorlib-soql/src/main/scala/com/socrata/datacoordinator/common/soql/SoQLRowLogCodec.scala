package com.socrata.datacoordinator.common.soql

import com.socrata.datacoordinator.truth.SimpleRowLogCodec
import com.google.protobuf.{CodedInputStream, CodedOutputStream}
import org.joda.time._
import com.socrata.soql.types._

object SoQLRowLogCodec extends SimpleRowLogCodec[SoQLValue] {
  def rowDataVersion: Short = 0

  // fixme; it'd be much better to do this in a manner simular to how column reps work

  protected def writeValue(target: CodedOutputStream, v: SoQLValue) {
    v match {
      case SoQLID(l) =>
        target.writeRawByte(0)
        target.writeInt64NoTag(l)
      case SoQLText(s) =>
        target.writeRawByte(1)
        target.writeStringNoTag(s)
      case SoQLNumber(bd) =>
        target.writeRawByte(2)
        target.writeStringNoTag(bd.toString)
      case SoQLMoney(bd) =>
        target.writeRawByte(3)
        target.writeStringNoTag(bd.toString)
      case SoQLBoolean(b) =>
        target.writeRawByte(4)
        target.writeBoolNoTag(b)
      case SoQLFixedTimestamp(ts) =>
        target.writeRawByte(5)
        target.writeStringNoTag(ts.getZone.getID)
        target.writeInt64NoTag(ts.getMillis)
      case SoQLLocation(lat, lon) =>
        target.writeRawByte(6)
        target.writeDoubleNoTag(lat)
        target.writeDoubleNoTag(lon)
      case SoQLFloatingTimestamp(ts) =>
        target.writeRawByte(7)
        target.writeStringNoTag(SoQLFloatingTimestamp.StringRep(ts))
      case SoQLDate(ts) =>
        target.writeRawByte(8)
        target.writeStringNoTag(SoQLDate.StringRep(ts))
      case SoQLTime(ts) =>
        target.writeRawByte(9)
        target.writeStringNoTag(SoQLTime.StringRep(ts))
      case SoQLNull =>
        target.writeRawByte(-1)
    }
  }

  protected def readValue(source: CodedInputStream): SoQLValue =
    source.readRawByte() match {
      case 0 =>
        SoQLID(source.readInt64())
      case 1 =>
        SoQLText(source.readString())
      case 2 =>
        SoQLNumber(new java.math.BigDecimal(source.readString()))
      case 3 =>
        SoQLMoney(new java.math.BigDecimal(source.readString()))
      case 4 =>
        SoQLBoolean.canonicalValue(source.readBool())
      case 5 =>
        val zone = DateTimeZone.forID(source.readString())
        SoQLFixedTimestamp(new DateTime(source.readInt64(), zone))
      case 6 =>
        val lat = source.readDouble()
        val lon = source.readDouble()
        SoQLLocation(lat, lon)
      case 7 =>
        SoQLFloatingTimestamp(SoQLFloatingTimestamp.StringRep.unapply(source.readString()).getOrElse {
          sys.error("Unable to parse floating timestamp from log!")
        })
      case 8 =>
        SoQLDate(SoQLDate.StringRep.unapply(source.readString()).getOrElse {
          sys.error("Unable to parse date from log!")
        })
      case 9 =>
        SoQLTime(SoQLTime.StringRep.unapply(source.readString()).getOrElse {
          sys.error("Unable to parse time from log!")
        })
      case -1 =>
        SoQLNull
    }
}
