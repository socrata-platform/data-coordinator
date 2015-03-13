package com.socrata.datacoordinator.truth.json

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec._

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

class CodecBasedJsonColumnRep[CT, CV, TrueCV : JsonDecode : JsonEncode](val representedType: CT, unwrapper: CV => TrueCV, wrapper: TrueCV => CV, NullValue: CV) extends JsonColumnRep[CT, CV] {
  def fromJValue(input: JValue) =
    if(input == JNull) Some(NullValue)
    else JsonDecode[TrueCV].decode(input).right.toOption.map(wrapper)

  def toJValue(input: CV) =
    if(NullValue == input) JNull
    else JsonEncode[TrueCV].encode(unwrapper(input))
}
