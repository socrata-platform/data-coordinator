package com.socrata.datacoordinator.common.soql.jsonreps

import com.socrata.datacoordinator.truth.json.CodecBasedJsonColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLText, SoQLType}
import com.socrata.soql.environment.ColumnName

class TextRep(name: ColumnName)
  extends CodecBasedJsonColumnRep[SoQLType, SoQLValue, String](name, SoQLText, _.asInstanceOf[SoQLText].value, SoQLText(_), SoQLNull)
