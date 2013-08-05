package com.socrata.datacoordinator.truth.json

import com.rojoma.json.ast._
import com.rojoma.json.codec._

trait JsonColumnCommonRep[CT, CV] {
  val representedType: CT
}

trait JsonColumnReadRep[CT, CV] extends JsonColumnCommonRep[CT, CV] {
  def fromJValue(input: JValue): Option[CV]
}

trait JsonColumnWriteRep[CT, CV] extends JsonColumnCommonRep[CT, CV] {
  def toJValue(value: CV): JValue
  protected def stdBadValue: Nothing = sys.error("Incorrect value passed to toJValue")
}

trait JsonColumnRep[CT, CV] extends JsonColumnReadRep[CT, CV] with JsonColumnWriteRep[CT, CV]

class CodecBasedJsonColumnRep[CT, CV, TrueCV : JsonCodec](val representedType: CT, unwrapper: CV => TrueCV, wrapper: TrueCV => CV, NullValue: CV) extends JsonColumnRep[CT, CV] {
  def fromJValue(input: JValue) =
    if(input == JNull) Some(NullValue)
    else JsonCodec[TrueCV].decode(input).map(wrapper)

  def toJValue(input: CV) =
    if(NullValue == input) JNull
    else JsonCodec[TrueCV].encode(unwrapper(input))
}
