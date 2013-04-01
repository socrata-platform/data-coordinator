package com.socrata.datacoordinator.common.soql.jsonreps

import com.socrata.datacoordinator.truth.json.CodecBasedJsonColumnRep
import com.socrata.soql.types.{SoQLLocation, SoQLType}
import com.socrata.datacoordinator.common.soql.{SoQLNullValue, SoQLLocationValue}
import com.socrata.soql.environment.ColumnName

class LocationRep(name: ColumnName) extends CodecBasedJsonColumnRep[SoQLType, Any, SoQLLocationValue](name, SoQLLocation, SoQLNullValue)
