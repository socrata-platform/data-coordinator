package com.socrata

package object datacoordinator {
  type Row[ColumnValue] = Map[String, ColumnValue]
}
