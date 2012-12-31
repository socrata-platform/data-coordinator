package com.socrata.datacoordinator.main

import com.socrata.datacoordinator.truth.metadata.{DatasetInfo, DatasetMapReader, GlobalLog, DatasetMapWriter}
import com.socrata.datacoordinator.truth.loader.{SchemaLoader, Logger}
import com.socrata.datacoordinator.manifest.TruthManifest
import org.joda.time.DateTime
import com.socrata.datacoordinator.util.IdProviderPool

abstract class DatabaseMutator[CT, CV] {
  trait ProviderOfNecessaryThings {
    val now: DateTime
    val datasetMapReader: DatasetMapReader
    val datasetMapWriter: DatasetMapWriter
    def datasetLog(ds: DatasetInfo): Logger[CV]
    val globalLog: GlobalLog
    val truthManifest: TruthManifest
    val idProviderPool: IdProviderPool
    def physicalColumnBaseForType(typ: CT): String
    def schemaLoader(version: datasetMapWriter.VersionInfo, logger: Logger[CV]): SchemaLoader
    def nameForType(typ: CT): String

    def singleId() = {
      val provider = idProviderPool.borrow()
      try {
        provider.allocate()
      } finally {
        idProviderPool.release(provider)
      }
    }
  }

  def withTransaction[T]()(f: ProviderOfNecessaryThings => T): T
}
