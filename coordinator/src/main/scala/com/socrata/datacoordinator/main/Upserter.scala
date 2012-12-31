package com.socrata.datacoordinator.main

import com.rojoma.simplearm.Managed

import com.socrata.datacoordinator.Row
import com.socrata.datacoordinator.truth.loader.{Report, Logger, Loader}
import com.socrata.datacoordinator.truth.metadata.VersionInfo

class Upserter[CT, CV](mutator: DatabaseMutator[CT, CV], loaderProvider: (VersionInfo, Logger[CV]) => Managed[Loader[CV]]) {
  def upsert(dataset: String, input: Iterator[Either[CV, Row[CV]]], username: String): Report[CV] = {
    mutator.withTransaction() { providerOfNecessaryThings =>
      import providerOfNecessaryThings._

      val ds = datasetMapReader.datasetInfo(dataset).getOrElse(sys.error("Augh no such dataset"))
      val table = datasetMapReader.latest(ds)
      val logger = datasetLog(ds)

      val report = for(loader <- loaderProvider(table, logger)) yield {
        for(op <- input) {
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
