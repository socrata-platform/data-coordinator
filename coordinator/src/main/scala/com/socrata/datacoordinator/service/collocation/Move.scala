package com.socrata.datacoordinator.service.collocation

import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, JsonKeyStrategy, Strategy}
import com.socrata.datacoordinator.id.DatasetInternalName

@JsonKeyStrategy(Strategy.Underscore)
case class Move(datasetInternalName: DatasetInternalName, storeIdFrom: String, storeIdTo: String)

object Move {
  implicit val encode = AutomaticJsonEncodeBuilder[Move]
}
