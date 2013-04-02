package com.socrata.datacoordinator.common.soql.jsonreps

import com.socrata.datacoordinator.truth.json.CodecBasedJsonColumnRep
import com.socrata.soql.types.{SoQLID, SoQLType}
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.common.soql.SoQLNullValue
import com.socrata.soql.environment.ColumnName
import com.rojoma.json.codec.JsonCodec

class IDRep(name: ColumnName)(implicit c: JsonCodec[RowId]) extends CodecBasedJsonColumnRep[SoQLType, Any, RowId](name, SoQLID, SoQLNullValue)
