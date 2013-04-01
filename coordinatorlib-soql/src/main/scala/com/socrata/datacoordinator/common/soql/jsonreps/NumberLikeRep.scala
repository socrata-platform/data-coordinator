package com.socrata.datacoordinator.common.soql.jsonreps

import com.socrata.datacoordinator.truth.json.CodecBasedJsonColumnRep
import com.socrata.soql.types.{SoQLBoolean, SoQLType}
import com.socrata.datacoordinator.common.soql.SoQLNullValue
import com.socrata.soql.environment.ColumnName

class NumberLikeRep(name: ColumnName, typ: SoQLType) extends CodecBasedJsonColumnRep[SoQLType, Any, BigDecimal](name, typ, SoQLNullValue)
