package com.socrata.datacoordinator.id

import java.sql.{ResultSet, Types, PreparedStatement}
import java.util.UUID
import com.socrata.datacoordinator.truth.metadata.LifecycleStage

package object sql {
  implicit class DatasetIdSetter(val __underlying: PreparedStatement) extends AnyVal {
    def setDatasetId(idx: Int, value: DatasetId): Unit = {
      val x: Long = value.underlying
      __underlying.setObject(idx, x, Types.OTHER)
    }
    def setLifecycleStage(idx: Int, value: LifecycleStage): Unit = {
      val x = value.name()
      __underlying.setString(idx, x)
    }
  }

  implicit class CopyIdSetter(val __underlying: PreparedStatement) extends AnyVal {
    def setCopyId(idx: Int, value: CopyId): Unit = {
      val x: Long = value.underlying
      __underlying.setObject(idx, x, Types.OTHER)
    }
  }

  implicit class CopyIdGetter(val __underlying: ResultSet) extends AnyVal {
    private def copyIdify(x: Long) =
      if(__underlying.wasNull) CopyId.Invalid
      else new CopyId(x)
    def getCopyId(col: String): CopyId =
      copyIdify(__underlying.getLong(col))
    def getCopyId(idx: Int): CopyId =
      copyIdify(__underlying.getLong(idx))
  }

  implicit class ColumnIdSetter(val __underlying: PreparedStatement) extends AnyVal {
    def setCopyId(idx: Int, value: ColumnId): Unit = {
      val x: Long = value.underlying
      __underlying.setObject(idx, x, Types.OTHER)
    }
  }

  implicit class ColumnIdGetter(val __underlying: ResultSet) extends AnyVal {
    private def columnIdify(x: Long) =
      if(__underlying.wasNull) ColumnId.Invalid
      else new ColumnId(x)
    def getColumnId(col: String): ColumnId =
      columnIdify(__underlying.getLong(col))
    def getColumnId(idx: Int): ColumnId =
      columnIdify(__underlying.getLong(idx))
  }

  implicit class DatasetIdGetter(val __underlying: ResultSet) extends AnyVal {
    private def datasetIdify(x: Long) =
      if(__underlying.wasNull) DatasetId.Invalid
      else new DatasetId(x)
    def getDatasetId(col: String): DatasetId =
      datasetIdify(__underlying.getLong(col))
    def getDatasetId(idx: Int): DatasetId =
      datasetIdify(__underlying.getLong(idx))

    private def lifecycleStageify(x: String) =
      if(x == null) null
      else LifecycleStage.valueOf(x)
    def getLifecycleStage(col: String): LifecycleStage =
      lifecycleStageify(__underlying.getString(col))
    def getLifecycleStage(idx: Int): LifecycleStage =
      lifecycleStageify(__underlying.getString(idx))

    def getNullableLong(col: String): Option[Long] = {
      val v = __underlying.getLong(col)
      if(v == 0 && __underlying.wasNull) None
      else Some(v)
    }
  }
}
