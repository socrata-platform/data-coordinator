package com.socrata.datacoordinator.common.soql

import com.socrata.soql.types._
import com.socrata.datacoordinator.truth.sql.SqlColumnRep

object SoQLRep {
  val repFactories = Map[SoQLType, String => SqlColumnRep[SoQLType, Any]](
    SoQLID -> (base => new IDRep(base)),
    SoQLText -> (base => new TextRep(base)),
    SoQLBoolean -> (base => new BooleanRep(base)),
    SoQLNumber -> (base => new NumberLikeRep(SoQLNumber, base)),
    SoQLMoney -> (base => new NumberLikeRep(SoQLNumber, base)),
    SoQLFixedTimestamp -> (base => new FixedTimestampRep(base)),
    SoQLLocation -> (base => new LocationRep(base)) /*,
    SoQLDouble -> doubleRepFactory,
    SoQLFloatingTimestamp -> floatingTimestampRepFactory,
    SoQLObject -> objectRepFactory,
    SoQLArray -> arrayRepFactory */
  )

  // for(typ <- SoQLType.typesByName.values) assert(repFactories.contains(typ))
}
