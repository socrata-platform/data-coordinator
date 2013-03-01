package com.socrata.datacoordinator.common.soql

import com.socrata.datacoordinator.truth.SimpleRowLogCodec
import com.google.protobuf.{CodedInputStream, CodedOutputStream}
import com.socrata.datacoordinator.id.RowId
import org.joda.time.{DateTimeZone, DateTime}

object SoQLRowLogCodec extends SimpleRowLogCodec[Any] {
  def rowDataVersion: Short = 0

  // fixme; it'd be much better to do this in a manner simular to how column reps work

  protected def writeValue(target: CodedOutputStream, v: Any) {
    v match {
      case l: RowId =>
        target.writeRawByte(0)
        target.writeInt64NoTag(l.underlying)
      case s: String =>
        target.writeRawByte(1)
        target.writeStringNoTag(s)
      case bd: BigDecimal =>
        target.writeRawByte(2)
        target.writeStringNoTag(bd.toString)
      case b: Boolean =>
        target.writeRawByte(3)
        target.writeBoolNoTag(b)
      case ts: DateTime =>
        target.writeRawByte(4)
        target.writeStringNoTag(ts.getZone.getID)
        target.writeInt64NoTag(ts.getMillis)
      case loc: SoQLLocationValue =>
        target.writeRawByte(5)
        target.writeDoubleNoTag(loc.latitude)
        target.writeDoubleNoTag(loc.longitude)
      case SoQLNullValue =>
        target.writeRawByte(-1)
    }
  }

  protected def readValue(source: CodedInputStream): Any =
    source.readRawByte() match {
      case 0 =>
        new RowId(source.readInt64())
      case 1 =>
        source.readString()
      case 2 =>
        BigDecimal(source.readString())
      case 3 =>
        source.readBool()
      case 4 =>
        val zone = DateTimeZone.forID(source.readString())
        new DateTime(source.readInt64(), zone)
      case 5 =>
        val lat = source.readDouble()
        val lon = source.readDouble()
        SoQLLocationValue(lat, lon)
      case -1 =>
        SoQLNullValue
    }
}
