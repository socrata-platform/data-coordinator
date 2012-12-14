package com.socrata.datacoordinator
package truth.sql

import java.sql.Connection
import truth.metadata.{DatasetMapReader, DatasetMapWriter}

trait TransactionProvider {
  def withReadOnlyTransaction[T](datasetId: String)(f: ReadConnectionInfo => T): T
  def withWriteTransaction[T](datasetId: String)(f: WriteConnectionInfo => T): T
}

trait ReadConnectionInfo {
  val datasetMapReader: DatasetMapReader
  val datasetInfo: Option[datasetMapReader.DatasetInfo]
  val connection: Connection
}

trait WriteConnectionInfo {
  val datasetMapWriter: DatasetMapWriter
  val datasetInfo: Option[datasetMapWriter.DatasetInfo]
  val connection: Connection
}
