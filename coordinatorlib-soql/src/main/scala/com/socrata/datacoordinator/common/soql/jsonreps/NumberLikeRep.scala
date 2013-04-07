package com.socrata.datacoordinator.common.soql.jsonreps

import com.socrata.datacoordinator.truth.json.CodecBasedJsonColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLType}
import com.socrata.soql.environment.ColumnName

class NumberLikeRep(name: ColumnName, typ: SoQLType, unwrapper: SoQLValue => java.math.BigDecimal, wrapper: java.math.BigDecimal => SoQLValue)
  extends CodecBasedJsonColumnRep[SoQLType, SoQLValue, java.math.BigDecimal](name, typ, unwrapper, wrapper, SoQLNull)
