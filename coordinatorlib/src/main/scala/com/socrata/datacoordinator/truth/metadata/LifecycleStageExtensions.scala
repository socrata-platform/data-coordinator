package com.socrata.datacoordinator.truth.metadata

import com.rojoma.json.v3.codec.{DecodeError, JsonEncode, JsonDecode}
import com.rojoma.json.v3.ast.{JString, JValue}

class LifecycleStageExtensions {
  implicit val jsonCodec = new JsonDecode[LifecycleStage] with JsonEncode[LifecycleStage] {
    def encode(x: LifecycleStage): JValue = JString(x.name)

    def decode(x: JValue) = x match {
      case JString(s) =>
        try {
          Right(LifecycleStage.valueOf(s))
        } catch {
          case _: IllegalArgumentException =>
            Left(DecodeError.InvalidValue(x))
        }
      case other =>
        Left(DecodeError.InvalidType(JString, other.jsonType))
    }
  }
}
