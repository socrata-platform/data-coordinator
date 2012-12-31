package com.socrata.datacoordinator.main

import com.socrata.datacoordinator.truth.metadata.{GlobalLog, DatasetMapWriter}
import com.socrata.datacoordinator.truth.loader.{SchemaLoader, Logger}
import com.socrata.datacoordinator.manifest.TruthManifest
import org.joda.time.DateTime
import com.socrata.datacoordinator.util.IdProviderPool

abstract class DatabaseMutator[CT, CV] {
  trait ProviderOfNecessaryThings {
    val now: DateTime
    val datasetMapWriter: DatasetMapWriter
    def datasetLog(ds: datasetMapWriter.DatasetInfo): Logger[CV]
    val globalLog: GlobalLog
    val truthManifest: TruthManifest
    val idProviderPool: IdProviderPool
    def physicalColumnBaseForType(typ: CT): String
    def loader(version: datasetMapWriter.VersionInfo): SchemaLoader
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
