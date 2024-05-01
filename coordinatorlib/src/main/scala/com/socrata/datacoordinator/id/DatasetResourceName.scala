package com.socrata.datacoordinator.id

import com.rojoma.json.v3.util.{WrapperJsonCodec, WrapperFieldCodec}

case class DatasetResourceName(underlying: String)

object DatasetResourceName {
  implicit val jCodec = WrapperJsonCodec[DatasetResourceName](DatasetResourceName(_), _.underlying)
  implicit val fCodec = WrapperFieldCodec[DatasetResourceName](DatasetResourceName(_), _.underlying)
}
