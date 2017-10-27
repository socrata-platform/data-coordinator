package com.socrata.datacoordinator.common.soql

import com.socrata.datacoordinator.truth.SimpleRowLogCodec
import com.google.protobuf.{ByteString, CodedInputStream, CodedOutputStream}
import org.joda.time._
import com.socrata.soql.types._
import com.rojoma.json.v3.io.{JsonReader, CompactJsonWriter}
import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.json.v3.ast.{JObject, JArray}

object SoQLRowLogCodec extends SimpleRowLogCodec[SoQLValue] {
  def rowDataVersion: Short = 0

  // fixme; it'd be much better to do this in a manner similar to how column reps work

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
      case SoQLFloatingTimestamp(ts) =>
        target.writeRawByte(7)
        target.writeStringNoTag(SoQLFloatingTimestamp.StringRep(ts))
      case SoQLDate(ts) =>
        target.writeRawByte(8)
        target.writeStringNoTag(SoQLDate.StringRep(ts))
      case SoQLTime(ts) =>
        target.writeRawByte(9)
        target.writeStringNoTag(SoQLTime.StringRep(ts))
      case SoQLArray(xs) =>
        target.writeRawByte(10)
        target.writeStringNoTag(CompactJsonWriter.toString(xs))
      case SoQLDouble(x) =>
        target.writeRawByte(11)
        target.writeDoubleNoTag(x)
      case SoQLJson(j) =>
        target.writeRawByte(12)
        target.writeStringNoTag(CompactJsonWriter.toString(j))
      case SoQLObject(j) =>
        target.writeRawByte(13)
        target.writeStringNoTag(CompactJsonWriter.toString(j))
      case SoQLVersion(ver) =>
        target.writeRawByte(14)
        target.writeInt64NoTag(ver)
      case SoQLPoint(p) =>
        target.writeRawByte(15)
        target.writeBytesNoTag(ByteString.copyFrom(SoQLPoint.WkbRep(p)))
      case SoQLMultiLine(ml) =>
        target.writeRawByte(16)
        target.writeBytesNoTag(ByteString.copyFrom(SoQLMultiLine.WkbRep(ml)))
      case SoQLMultiPolygon(mp) =>
        target.writeRawByte(17)
        target.writeBytesNoTag(ByteString.copyFrom(SoQLMultiPolygon.WkbRep(mp)))
      case SoQLPolygon(p) =>
        target.writeRawByte(18)
        target.writeBytesNoTag(ByteString.copyFrom(SoQLPolygon.WkbRep(p)))
      case SoQLLine(l) =>
        target.writeRawByte(19)
        target.writeBytesNoTag(ByteString.copyFrom(SoQLLine.WkbRep(l)))
      case SoQLMultiPoint(mp) =>
        target.writeRawByte(20)
        target.writeBytesNoTag(ByteString.copyFrom(SoQLMultiPoint.WkbRep(mp)))
      case SoQLBlob(id) =>
        target.writeRawByte(21)
        target.writeStringNoTag(id)
      case SoQLLocation(lat, lng, address) =>
        target.writeRawByte(22)
        target.writeBoolNoTag(lat.isDefined)
        if (lat.isDefined) {
          target.writeStringNoTag(lat.map(_.toString).get)
          target.writeStringNoTag(lng.map(_.toString).get)
        }
        target.writeBoolNoTag(address.isDefined)
        if (address.isDefined) {
          target.writeStringNoTag(address.get)
        }
      case SoQLPhone(phoneNumber, phoneType) =>
        target.writeRawByte(23)
        target.writeBoolNoTag(phoneNumber.isDefined)
        if (phoneNumber.isDefined) {
          target.writeStringNoTag(phoneNumber.get)
        }
        target.writeBoolNoTag(phoneType.isDefined)
        if (phoneType.isDefined) {
          target.writeStringNoTag(phoneType.get)
        }
      case SoQLUrl(url, description) =>
        target.writeRawByte(24)
        target.writeBoolNoTag(url.isDefined)
        if (url.isDefined) {
          target.writeStringNoTag(url.get)
        }
        target.writeBoolNoTag(description.isDefined)
        if (description.isDefined) {
          target.writeStringNoTag(description.get)
        }
      case x@SoQLDocument(_, _, _) =>
        target.writeRawByte(25)
        target.writeStringNoTag(JsonUtil.renderJson(x, false))
      case SoQLPhoto(id) =>
        target.writeRawByte(26)
        target.writeStringNoTag(id)
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
        throw new RuntimeException("SoQLLocation is no longer supported")
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
      case 10 =>
        SoQLArray(JsonUtil.parseJson[JArray](source.readString()).right.toOption.getOrElse {
          sys.error("Unable to parse array from log!")
        })
      case 11 =>
        SoQLDouble(source.readDouble())
      case 12 =>
        SoQLJson(JsonReader.fromString(source.readString()))
      case 13 =>
        SoQLObject(JsonUtil.parseJson[JObject](source.readString()).right.toOption.getOrElse {
          sys.error("Unable to parse object from log!")
        })
      case 14 =>
        SoQLVersion(source.readInt64())
      case 15 =>
        SoQLPoint.WkbRep.unapply(source.readBytes.toByteArray) match {
          case Some(point) => SoQLPoint(point)
          case _ => sys.error("Unable to parse object from log!")
        }
      case 16 =>
        SoQLMultiLine.WkbRep.unapply(source.readBytes.toByteArray) match {
          case Some(multiline) => SoQLMultiLine(multiline)
          case _ => sys.error("Unable to parse object from log!")
        }
      case 17 =>
        SoQLMultiPolygon.WkbRep.unapply(source.readBytes.toByteArray) match {
          case Some(multipolygon) => SoQLMultiPolygon(multipolygon)
          case _ => sys.error("Unable to parse object from log!")
        }
      case 18 =>
        SoQLPolygon.WkbRep.unapply(source.readBytes.toByteArray) match {
          case Some(polygon) => SoQLPolygon(polygon)
          case _ => sys.error("Unable to parse object from log!")
        }
      case 19 =>
        SoQLLine.WkbRep.unapply(source.readBytes.toByteArray) match {
          case Some(line) => SoQLLine(line)
          case _ => sys.error("Unable to parse object from log!")
        }
      case 20 =>
        SoQLMultiPoint.WkbRep.unapply(source.readBytes.toByteArray) match {
          case Some(multipoint) => SoQLMultiPoint(multipoint)
          case _ => sys.error("Unable to parse object from log!")
        }
      case 21 =>
        SoQLBlob(source.readString())
      case 22 =>
        val (lat, lng) =
          if (source.readBool()) {
            (Option(source.readString()).map(new java.math.BigDecimal(_)),
             Option(source.readString()).map(new java.math.BigDecimal(_)))
          } else {
            (None, None)
          }
        val address =
          if (source.readBool()) Option(source.readString())
          else None
        SoQLLocation(lat, lng, address)
      case 23 =>
        val phoneNumber =
          if (source.readBool()) { Option(source.readString()) }
          else { None }
        val phoneType =
          if (source.readBool()) { Option(source.readString()) }
          else { None }
        SoQLPhone(phoneNumber, phoneType)
      case 24 =>
        val url =
          if (source.readBool()) { Option(source.readString()) }
          else { None }
        val description =
          if (source.readBool()) { Option(source.readString()) }
          else { None }
        SoQLUrl(url, description)
      case 25 =>
        val x = source.readString()
        JsonUtil.parseJson[SoQLDocument](x).right.getOrElse(sys.error("Unable to parse document from log!"))
      case 26 =>
        SoQLPhoto(source.readString())
      case -1 =>
        SoQLNull
    }
}
