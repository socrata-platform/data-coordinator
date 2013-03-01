package com.socrata.datacoordinator.common.soql.jsonreps

import com.socrata.datacoordinator.truth.json.CodecBasedJsonColumnRep
import com.socrata.soql.types.{SoQLID, SoQLType}
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.common.soql.SoQLNullValue

class IDRep(name: String) extends CodecBasedJsonColumnRep[SoQLType, Any, RowId](name, SoQLID, SoQLNullValue)
