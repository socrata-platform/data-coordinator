package com.socrata.datacoordinator.common.soql.jsonreps

import com.socrata.datacoordinator.truth.json.CodecBasedJsonColumnRep
import com.socrata.soql.types.{SoQLLocation, SoQLType}
import com.socrata.datacoordinator.common.soql.{SoQLNullValue, SoQLLocationValue}

class LocationRep(name: String) extends CodecBasedJsonColumnRep[SoQLType, Any, SoQLLocationValue](name, SoQLLocation, SoQLNullValue)
