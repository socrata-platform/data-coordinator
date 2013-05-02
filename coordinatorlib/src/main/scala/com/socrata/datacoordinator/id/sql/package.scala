package com.socrata.datacoordinator.id

import java.sql.{ResultSet, Types, PreparedStatement}
import java.util.UUID

package object sql {
  implicit class DatasetIdSetter(val __underlying: PreparedStatement) extends AnyVal {
    def setDatasetId(idx: Int, value: DatasetId) {
      val x: Long = value.underlying
      __underlying.setObject(idx, x, Types.OTHER)
    }
  }

  implicit class DatasetIdGetter(val __underlying: ResultSet) extends AnyVal {
    private def datasetIdify(x: Long) =
      if(__underlying.wasNull) DatasetId.Invalid
      else new DatasetId(x)
    def getDatasetId(col: String): DatasetId =
      datasetIdify(__underlying.getLong(col))
    def getDatasetId(idx: Int): DatasetId =
      datasetIdify(__underlying.getLong(idx))
  }
}
