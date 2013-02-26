package com.socrata.datacoordinator.truth.json

import scala.reflect.ClassTag

import com.rojoma.json.ast._
import com.rojoma.json.io._
import com.rojoma.json.codec._

trait JsonColumnCommonRep[CT, CV] {
  val name: String
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

class CodecBasedJsonColumnRep[CT, CV, TrueCV <: CV : ClassTag : JsonCodec](val name: String, val representedType: CT, NullValue: CV) extends JsonColumnRep[CT, CV] {
  def fromJValue(input: JValue) =
    if(input == JNull) Some(NullValue)
    else JsonCodec[TrueCV].decode(input)

  def toJValue(input: CV) = input match {
    case trueCV: TrueCV => JsonCodec[TrueCV].encode(trueCV)
    case NullValue => JNull
    case _ => stdBadValue
  }
}
