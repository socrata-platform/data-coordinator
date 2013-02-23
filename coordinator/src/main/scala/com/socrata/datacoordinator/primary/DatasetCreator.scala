package com.socrata.datacoordinator.primary

import scalaz._
import scalaz.effect._
import Scalaz._

import com.socrata.soql.brita.AsciiIdentifierFilter
import com.socrata.datacoordinator.truth.MonadicDatasetMutator

class DatasetCreator[CT, CV](mutator: MonadicDatasetMutator[CV], nameForType: CT => String, systemColumns: Map[String, CT], idColumnName: String) {
  import mutator._

  private val addSystemColumns = DatasetCreator.addSystemColumns(mutator, nameForType, systemColumns, idColumnName)

  def createDataset(datasetId: String, username: String): IO[Unit] = {
    creatingDataset(as = username)(datasetId, "t") {
      addSystemColumns
    }
  }
}

object DatasetCreator {
  def addSystemColumns[CT, CV](mutator: MonadicDatasetMutator[CV], nameForType: CT => String, systemColumns: Map[String, CT], idColumnName: String): mutator.DatasetM[Unit] =
    systemColumns.map { case (name, typ) =>
      import mutator._
      addColumn(name, nameForType(typ), AsciiIdentifierFilter(List("s", name)).toLowerCase).flatMap { col =>
        if(col.logicalName == idColumnName) makeSystemPrimaryKey(col)
        else col.pure[DatasetM]
      }
    }.toList.sequenceU.map(_ => ())
}
