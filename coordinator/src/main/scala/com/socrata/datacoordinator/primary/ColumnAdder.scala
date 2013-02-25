package com.socrata.datacoordinator.primary

import scalaz._
import scalaz.effect._
import Scalaz._
import com.socrata.soql.brita.AsciiIdentifierFilter

import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.{DataWritingContext, MonadicDatasetMutator}

class ColumnAdder[CT] private (val dataContext: DataWritingContext) extends ExistingDatasetMutator {
  import dataContext.datasetMutator._

  def addToSchema(dataset: String, columns: Map[String, CT], username: String): IO[Map[String, ColumnInfo]] = {
    val columnCreations = columns.iterator.map { case (columnName, columnType) =>
      val baseName = dataContext.physicalColumnBaseBase(columnName)
      addColumn(columnName, dataContext.typeContext.nameFromType(columnType.asInstanceOf[dataContext.CT] /* SI-5712, see below */), baseName)
    }.toList.sequence

    withDataset(as = username)(dataset) {
      for {
        newColumns <- columnCreations
      } yield newColumns.foldLeft(Map.empty[String, ColumnInfo]) {
        (acc, ci) => acc + (ci.logicalName -> ci)
      }
    }.flatMap(finish(dataset))
  }
}

object ColumnAdder {
  // cannot pass the evidence into ColumnAdder's ctor because of SI-5712.  So instead just
  // _require_ the evidence and then do a manual cast, which the evidence we're given
  // has proven is safe.  This is also why ColumnAdder's ctor is private; once that bug
  // is fixed this companion object can go away.
  def apply[CT](dataContext: DataWritingContext)(implicit ev: CT =:= dataContext.CT) = new ColumnAdder[CT](dataContext)
}
