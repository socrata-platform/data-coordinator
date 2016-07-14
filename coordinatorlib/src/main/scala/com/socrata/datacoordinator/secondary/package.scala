package com.socrata.datacoordinator

import com.socrata.datacoordinator.util.collection.ColumnIdMap

package object secondary {
  type Row[CV] = ColumnIdMap[CV]
}
