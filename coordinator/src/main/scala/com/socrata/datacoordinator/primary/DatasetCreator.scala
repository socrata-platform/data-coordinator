package com.socrata.datacoordinator.primary

import scalaz._
import scalaz.effect._
import Scalaz._

import com.socrata.soql.brita.AsciiIdentifierFilter
import com.socrata.datacoordinator.truth.MonadicDatasetMutator

class DatasetCreator[CT, CV](mutator: MonadicDatasetMutator[CV], nameForType: CT => String, systemColumns: Map[String, CT], idColumnName: String) {
  import mutator._

  def createDataset(datasetId: String, username: String): IO[Unit] = {
    val systemColumnAdds = systemColumns.iterator.map { case (name, typ) =>
      addColumn(name, nameForType(typ), AsciiIdentifierFilter(List("s", name)).toLowerCase).flatMap { col =>
        if(col.logicalName == idColumnName) makeSystemPrimaryKey(col)
        else col.pure[DatasetM]
      }
    }.toStream
    creatingDataset(as = username)(datasetId, "t") {
      systemColumnAdds.sequenceU.map(_ => ())
    }
  }
}
