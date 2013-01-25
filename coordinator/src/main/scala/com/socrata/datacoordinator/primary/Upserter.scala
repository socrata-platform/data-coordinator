package com.socrata.datacoordinator.primary

import com.rojoma.simplearm.Managed

import com.socrata.datacoordinator.Row
import com.socrata.datacoordinator.truth.loader.Report
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.ColumnInfo

class Upserter[CT, CV](mutator: DatabaseMutator[CT, CV]) {
  def upsert(dataset: String, username: String)(inputGenerator: ColumnIdMap[ColumnInfo] => Managed[Iterator[Either[CV, Row[CV]]]]): Report[CV] = {
    mutator.withDataUpdate(dataset, username) { providerOfNecessaryThings =>
      import providerOfNecessaryThings._

      for {
        it <- inputGenerator(initialSchema)
        op <- it
      } {
        op match {
          case Right(row) => dataLoader.upsert(row)
          case Left(id) => dataLoader.delete(id)
        }
      }

      dataLoader.report
    }
  }
}
