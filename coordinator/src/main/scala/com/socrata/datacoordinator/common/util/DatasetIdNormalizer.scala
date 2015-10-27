package com.socrata.datacoordinator.common.util

import com.ibm.icu.text.Normalizer
import com.rojoma.json.v3.io.{IdentifierEvent, FieldEvent, StringEvent, JsonEvent}

object DatasetIdNormalizer {

  val normalizationMode: Normalizer.Mode = Normalizer.NFC

  def norm(s: String) = Normalizer.normalize(s, normalizationMode)

  def normalizeJson(token: JsonEvent): JsonEvent = {
    def position(t: JsonEvent) = t.positionedAt(token.position)
    token match {
      case StringEvent(s) =>
        position(StringEvent(norm(s))(token.position))
      case FieldEvent(s) =>
        position(FieldEvent(norm(s))(token.position))
      case IdentifierEvent(s) =>
        position(IdentifierEvent(norm(s))(token.position))
      case other =>
        other
    }
  }

}
