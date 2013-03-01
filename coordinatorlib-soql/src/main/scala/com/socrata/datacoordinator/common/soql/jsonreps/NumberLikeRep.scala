package com.socrata.datacoordinator.common.soql.jsonreps

import com.socrata.datacoordinator.truth.json.CodecBasedJsonColumnRep
import com.socrata.soql.types.{SoQLBoolean, SoQLType}
import com.socrata.datacoordinator.common.soql.SoQLNullValue

class NumberLikeRep(name: String, typ: SoQLType) extends CodecBasedJsonColumnRep[SoQLType, Any, BigDecimal](name, typ, SoQLNullValue)
