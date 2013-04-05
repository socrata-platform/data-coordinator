package com.socrata.datacoordinator.common.soql.jsonreps

import com.socrata.datacoordinator.truth.json.CodecBasedJsonColumnRep
import com.socrata.soql.types.SoQLType
import com.socrata.datacoordinator.common.soql.{SoQLValue, SoQLNullValue}
import com.socrata.soql.environment.ColumnName

class NumberLikeRep(name: ColumnName, typ: SoQLType, unwrapper: SoQLValue => java.math.BigDecimal, wrapper: java.math.BigDecimal => SoQLValue)
  extends CodecBasedJsonColumnRep[SoQLType, SoQLValue, java.math.BigDecimal](name, typ, unwrapper, wrapper, SoQLNullValue)
