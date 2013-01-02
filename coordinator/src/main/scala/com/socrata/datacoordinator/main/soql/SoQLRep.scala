package com.socrata.datacoordinator.main.soql

import com.socrata.soql.types._
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.id.ColumnId

object SoQLRep {
  val repFactories = Map[SoQLType, String => SqlColumnRep[SoQLType, Any]](
    SoQLText -> TextRepFactory,
    SoQLBoolean -> BooleanRepFactory,
    SoQLNumber -> new NumberLikeFactory(SoQLNumber),
    SoQLMoney -> new NumberLikeFactory(SoQLNumber),
    SoQLFixedTimestamp -> FixedTimestampRepFactory /*,
    SoQLDouble -> doubleRepFactory,
    SoQLFloatingTimestamp -> floatingTimestampRepFactory,
    SoQLObject -> objectRepFactory,
    SoQLArray -> arrayRepFactory,
    SoQLLocation -> locationRepFactory */
  )

  // for(typ <- SoQLType.typesByName.values) assert(repFactories.contains(typ))
}
