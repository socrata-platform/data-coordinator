package com.socrata.datacoordinator.primary

import scalaz._
import scalaz.effect._
import Scalaz._
import com.socrata.soql.brita.AsciiIdentifierFilter

import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.{DataWritingContext, MonadicDatasetMutator}

class ColumnAdder[CT](val dataContext: DataWritingContext[CT, _]) extends ExistingDatasetMutator {
  import dataContext.datasetMutator._

  def addToSchema(dataset: String, columns: Map[String, CT], username: String): IO[Map[String, ColumnInfo]] = {
    val columnCreations = columns.iterator.map { case (columnName, columnType) =>
      val baseName = dataContext.physicalColumnBaseBase(columnName)
      addColumn(columnName, dataContext.typeContext.nameFromType(columnType), baseName)
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
