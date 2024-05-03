package com.socrata.datacoordinator
package truth

import com.rojoma.json.v3.ast.JNull
import com.rojoma.json.v3.util.{AllowMissing, AutomaticJsonCodecBuilder}

import com.socrata.soql.analyzer2._
import com.socrata.soql.stdlib.analyzer2.UserParameters

case class RollupShape(
  foundTables: FoundTables[RollupMetaTypes],
  @AllowMissing("Map.empty") locationSubcolumns: Map[
    types.DatabaseTableName[RollupMetaTypes],
    Map[
      types.DatabaseColumnName[RollupMetaTypes],
      Seq[
        Either[JNull, types.DatabaseColumnName[RollupMetaTypes]]
      ]
    ]
  ],
  @AllowMissing("Nil") rewritePasses: Seq[Seq[rewrite.AnyPass]],
  @AllowMissing("UserParameters.empty") userParameters: UserParameters
)

object RollupShape {
  implicit val codec = AutomaticJsonCodecBuilder[RollupShape]
}
