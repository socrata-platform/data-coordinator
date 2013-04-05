package com.socrata.datacoordinator.common.soql.jsonreps

import com.socrata.datacoordinator.truth.json.CodecBasedJsonColumnRep
import com.socrata.soql.types.{SoQLText, SoQLType}
import com.socrata.datacoordinator.common.soql.{SoQLTextValue, SoQLValue, SoQLNullValue}
import com.socrata.soql.environment.ColumnName

class TextRep(name: ColumnName)
  extends CodecBasedJsonColumnRep[SoQLType, SoQLValue, String](name, SoQLText, _.asInstanceOf[SoQLTextValue].value, SoQLTextValue(_), SoQLNullValue)
