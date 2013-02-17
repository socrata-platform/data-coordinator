package com.socrata.datacoordinator.primary

import scalaz._
import scalaz.effect._
import Scalaz._
import org.joda.time.DateTime
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo}
import com.socrata.datacoordinator.util.collection.ColumnIdMap

trait MonadicDatabaseMutator {
  type MutationContext
  type DatasetM[T] = StateT[IO, MutationContext, T]

  def runTransaction[A](datasetId: String)(action: DatasetM[A]): IO[Option[A]]

  def now: DatasetM[DateTime]
  def copyInfo: DatasetM[CopyInfo]
  def schema: DatasetM[ColumnIdMap[ColumnInfo]]
  def io[A](op: => A): DatasetM[A]

  def addColumn(logicalName: String, typeName: String, physicalColumnBaseBase: String): DatasetM[ColumnInfo]
  def dropColumn(ci: ColumnInfo): DatasetM[Unit]
}
