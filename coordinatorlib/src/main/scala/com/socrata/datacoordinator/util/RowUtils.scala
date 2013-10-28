package com.socrata.datacoordinator.util

import com.socrata.datacoordinator.Row

object RowUtils {
  def delta[CV](oldRow: Row[CV], newRow: Row[CV]): Row[CV] =
    newRow.filterNot { (cid, v) =>
      Some(v) == oldRow.get(cid)
    }
}
