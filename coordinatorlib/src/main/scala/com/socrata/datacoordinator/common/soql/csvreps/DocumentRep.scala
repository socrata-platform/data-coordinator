package com.socrata.datacoordinator.common.soql.csvreps

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.truth.csv.CsvColumnRep
import com.socrata.soql.types._

object DocumentRep extends CsvColumnRep[SoQLType, SoQLValue] {
  val size = 1

  val representedType = SoQLDocument

  def decode(row: IndexedSeq[String], indices: IndexedSeq[Int]) = {
    assert(indices.size == size)
    Option(row(indices(0))) match {
      case Some(x) =>
        JsonUtil.parseJson[SoQLDocument](x) match {
          case Right(v) => Some(v)
          case _ => Some(SoQLNull)
        }
      case None =>
        None
    }
  }
}