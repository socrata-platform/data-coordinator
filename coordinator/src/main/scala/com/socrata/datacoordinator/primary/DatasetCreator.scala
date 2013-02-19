package com.socrata.datacoordinator.primary

import scalaz._
import scalaz.effect._
import Scalaz._

import com.socrata.soql.brita.AsciiIdentifierFilter
import com.socrata.datacoordinator.truth.MonadicDatasetMutator

class DatasetCreator[CT, CV](mutator: MonadicDatasetMutator[CV], nameForType: CT => String, systemColumns: Map[String, CT], idColumnName: String) {
  import mutator._

  private val addSystemColumns = systemColumns.map { case (name, typ) =>
    addColumn(name, nameForType(typ), AsciiIdentifierFilter(List("s", name)).toLowerCase).flatMap { col =>
      if(col.logicalName == idColumnName) makeSystemPrimaryKey(col)
      else col.pure[DatasetM]
    }
  }.toList.sequenceU.map(_ => ())

  def createDataset(datasetId: String, username: String): IO[Unit] = {
    creatingDataset(as = username)(datasetId, "t") {
      addSystemColumns
    }
  }
}
