package com.socrata.datacoordinator.common.soql

import com.socrata.soql.types._
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.truth.csv.CsvColumnRep

object SoQLRep {
  val sqlRepFactories = Map[SoQLType, String => SqlColumnRep[SoQLType, Any]](
    SoQLID -> (base => new sqlreps.IDRep(base)),
    SoQLText -> (base => new sqlreps.TextRep(base)),
    SoQLBoolean -> (base => new sqlreps.BooleanRep(base)),
    SoQLNumber -> (base => new sqlreps.NumberLikeRep(SoQLNumber, base)),
    SoQLMoney -> (base => new sqlreps.NumberLikeRep(SoQLNumber, base)),
    SoQLFixedTimestamp -> (base => new sqlreps.FixedTimestampRep(base)),
    SoQLLocation -> (base => new sqlreps.LocationRep(base)) /*,
    SoQLDouble -> doubleRepFactory,
    SoQLFloatingTimestamp -> floatingTimestampRepFactory,
    SoQLObject -> objectRepFactory,
    SoQLArray -> arrayRepFactory */
  )

  // for(typ <- SoQLType.typesByName.values) assert(repFactories.contains(typ))

  val csvRepFactories = Map[SoQLType, CsvColumnRep[SoQLType, Any]](
    SoQLID -> csvreps.IDRep,
    SoQLText -> csvreps.TextRep,
    SoQLBoolean -> csvreps.BooleanRep,
    SoQLNumber -> new csvreps.NumberLikeRep(SoQLNumber),
    SoQLMoney -> new csvreps.NumberLikeRep(SoQLMoney),
    SoQLFixedTimestamp -> csvreps.FixedTimestampRep,
    SoQLLocation -> csvreps.LocationRep
  )
}
