package com.socrata.datacoordinator.main.soql

import com.socrata.soql.types._
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.id.ColumnId

class SoQLRep {
  val repFactories = Map[SoQLType, ColumnId => SqlColumnRep[SoQLType, Any]](
    SoQLText -> TextRepFactory,
    SoQLBoolean -> BooleanRepFactory,
    SoQLNumber -> new NumberLikeFactory(SoQLNumber, "num"),
    SoQLMoney -> new NumberLikeFactory(SoQLNumber, "money") /*,
    SoQLDouble -> doubleRepFactory,
    SoQLFixedTimestamp -> fixedTimestampRepFactory,
    SoQLFloatingTimestamp -> floatingTimestampRepFactory,
    SoQLObject -> objectRepFactory,
    SoQLArray -> arrayRepFactory,
    SoQLLocation -> locationRepFactory */
  )

  for(typ <- SoQLType.typesByName.values) assert(repFactories.contains(typ))
}
