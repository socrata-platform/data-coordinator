package com.socrata.datacoordinator.primary

import scalaz._
import scalaz.effect._
import Scalaz._
import com.socrata.soql.brita.AsciiIdentifierFilter

import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.MonadicDatasetMutator

class ColumnAdder[CT, CV](mutator: MonadicDatasetMutator[CV], nameForType: CT => String, physicalColumnBaseLimit: Int) extends ExistingDatasetMutator {
  import mutator._

  def addToSchema(dataset: String, columns: Map[String, CT], username: String): IO[Map[String, ColumnInfo]] = {
    val columnCreations = columns.iterator.map { case (columnName, columnType) =>
      val baseName = AsciiIdentifierFilter(List("u", columnName)).take(physicalColumnBaseLimit).replaceAll("_+$","").toLowerCase
      addColumn(columnName, nameForType(columnType), baseName)
    }.toStream

    mutator.withDataset(as = username)(dataset) {
      columnCreations.sequence.map { _.foldLeft(Map.empty[String, ColumnInfo]) { (acc, ci) => acc + (ci.logicalName -> ci) } }
    }.flatMap(finish(dataset))
  }
}
