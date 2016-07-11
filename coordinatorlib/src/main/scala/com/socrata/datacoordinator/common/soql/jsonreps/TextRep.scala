package com.socrata.datacoordinator.common.soql.jsonreps

import com.socrata.datacoordinator.truth.json.CodecBasedJsonColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLText, SoQLType}
import com.socrata.soql.environment.ColumnName

object TextRep extends CodecBasedJsonColumnRep[SoQLType, SoQLValue, String](SoQLText, _.asInstanceOf[SoQLText].value, SoQLText(_), SoQLNull)
