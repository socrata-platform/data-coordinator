package com.socrata.datacoordinator.common.soql.csvreps

import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.json.v3.ast.JValue
import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types._

object JsonRep extends CsvColumnRep[SoQLType, SoQLValue] {
  val size = 1

  val representedType = SoQLJson

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]) = {
    assert(indices.size == size)
    Option(row(indices(0))) match {
      case Some(x) =>
        JsonUtil.parseJson[JValue](x) match {
          case Right(v) => Some(SoQLJson(v))
          case _ => Some(SoQLNull)
        }
      case None =>
        None
    }
  }
}
