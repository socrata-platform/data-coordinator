package com.socrata.datacoordinator.common.soql.jsonreps

import com.socrata.datacoordinator.truth.json.CodecBasedJsonColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLPhoto, SoQLType}

object PhotoRep extends CodecBasedJsonColumnRep[SoQLType, SoQLValue, String](SoQLPhoto, _.asInstanceOf[SoQLPhoto].value, SoQLPhoto(_), SoQLNull)
