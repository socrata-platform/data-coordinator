package com.socrata.datacoordinator.truth

import com.rojoma.json.v3.ast.JNull
import com.rojoma.json.v3.util.{AllowMissing, AutomaticJsonCodecBuilder}

import com.socrata.soql.analyzer2._
import com.socrata.soql.stdlib.analyzer2.UserParameters

case class RollupShape(
  foundTables: FoundTables[UnstagedDataCoordinatorMetaTypes],
  @AllowMissing("Map.empty") locationSubcolumns: Map[
    types.DatabaseTableName[UnstagedDataCoordinatorMetaTypes],
    Map[
      types.DatabaseColumnName[UnstagedDataCoordinatorMetaTypes],
      Seq[
        Either[JNull, types.DatabaseColumnName[UnstagedDataCoordinatorMetaTypes]]
      ]
    ]
  ],
  @AllowMissing("Nil") rewritePasses: Seq[Seq[rewrite.AnyPass]],
  @AllowMissing("UserParameters.empty") userParameters: UserParameters
)

object RollupShape {
  implicit val jCodec = AutomaticJsonCodecBuilder[RollupShape]
}
