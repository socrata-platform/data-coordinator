package com.socrata.datacoordinator.main

import com.rojoma.simplearm.Managed

import com.socrata.datacoordinator.Row
import com.socrata.datacoordinator.truth.loader.Report
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.ColumnInfo

class Upserter[CT, CV](mutator: DatabaseMutator[CT, CV]) {
  def upsert(dataset: String, username: String)(inputGenerator: ColumnIdMap[ColumnInfo] => Managed[Iterator[Either[CV, Row[CV]]]]): Report[CV] = {
    mutator.withTransaction() { providerOfNecessaryThings =>
      import providerOfNecessaryThings._

      val ds = datasetMapReader.datasetInfo(dataset).getOrElse(sys.error("Augh no such dataset"))
      val table = datasetMapReader.latest(ds)
      val schema = datasetMapReader.schema(table)
      val logger = datasetLog(ds)

      val report = for(loader <- dataLoader(table, schema, logger)) yield {
        for {
          it <- inputGenerator(schema)
          op <- it
        } {
          op match {
            case Right(row) => loader.upsert(row)
            case Left(id) => loader.delete(id)
          }
        }
        loader.report
      }

      for(newVersion <- logger.endTransaction()) {
        truthManifest.updateLatestVersion(ds, newVersion)
        globalLog.log(ds, newVersion, now, username)
      }

      report
    }
  }
}
