package com.socrata.datacoordinator.main

import org.joda.time.DateTime
import com.rojoma.simplearm.Managed

import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.loader.{RowPreparer, Loader, SchemaLoader, Logger}
import com.socrata.datacoordinator.manifest.TruthManifest
import com.socrata.datacoordinator.util.IdProviderPool
import com.socrata.datacoordinator.util.collection.ColumnIdMap

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

    def dataLoader(table: VersionInfo, schema: ColumnIdMap[ColumnInfo], logger: Logger[CV]): Managed[Loader[CV]]
    def rowPreparer(schema: ColumnIdMap[ColumnInfo]): RowPreparer[CV]

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
