package com.socrata.datacoordinator.common.soql.jsonreps

import com.socrata.datacoordinator.truth.json.CodecBasedJsonColumnRep
import com.socrata.soql.types.{SoQLText, SoQLType}
import com.socrata.datacoordinator.common.soql.SoQLNullValue

class TextRep(name: String) extends CodecBasedJsonColumnRep[SoQLType, Any, String](name, SoQLText, SoQLNullValue)
