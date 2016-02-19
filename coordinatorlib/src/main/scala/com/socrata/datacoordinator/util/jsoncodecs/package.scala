package com.socrata.datacoordinator.util

import com.rojoma.json.v3.util.WrapperJsonCodec
import com.socrata.soql.environment.ColumnName

package object jsoncodecs {
  implicit val columnNameCodec = WrapperJsonCodec[ColumnName](ColumnName, _.name)
}
