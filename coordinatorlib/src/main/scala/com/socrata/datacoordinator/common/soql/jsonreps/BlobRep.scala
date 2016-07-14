package com.socrata.datacoordinator.common.soql.jsonreps

import com.socrata.datacoordinator.truth.json.CodecBasedJsonColumnRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLBlob, SoQLType}

object BlobRep extends CodecBasedJsonColumnRep[SoQLType, SoQLValue, String](SoQLBlob, _.asInstanceOf[SoQLBlob].value, SoQLBlob(_), SoQLNull)
