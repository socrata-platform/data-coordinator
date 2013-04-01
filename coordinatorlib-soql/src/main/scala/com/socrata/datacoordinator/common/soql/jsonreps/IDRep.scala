package com.socrata.datacoordinator.common.soql.jsonreps

import com.socrata.datacoordinator.truth.json.CodecBasedJsonColumnRep
import com.socrata.soql.types.{SoQLID, SoQLType}
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.common.soql.SoQLNullValue
import com.socrata.soql.environment.ColumnName

class IDRep(name: ColumnName) extends CodecBasedJsonColumnRep[SoQLType, Any, RowId](name, SoQLID, SoQLNullValue)
