package com.socrata.datacoordinator.common.soql.jsonreps

import com.socrata.datacoordinator.truth.json.CodecBasedJsonColumnRep
import com.socrata.soql.types.{SoQLText, SoQLType}
import com.socrata.datacoordinator.common.soql.SoQLNullValue
import com.socrata.soql.environment.ColumnName

class TextRep(name: ColumnName) extends CodecBasedJsonColumnRep[SoQLType, Any, String](name, SoQLText, SoQLNullValue)
