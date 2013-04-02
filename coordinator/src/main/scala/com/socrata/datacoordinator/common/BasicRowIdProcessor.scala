package com.socrata.datacoordinator.common

import com.socrata.datacoordinator.id.RowIdProcessor

object BasicRowIdProcessor extends RowIdProcessor(2, "*" + _ + "*", { s =>
  try {
    s.substring(1, s.length - 1).toLong
  } catch {
    case _: Exception =>
      -1
  }
})
