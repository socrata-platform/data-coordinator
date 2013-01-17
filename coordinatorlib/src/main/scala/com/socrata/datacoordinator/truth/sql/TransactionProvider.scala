package com.socrata.datacoordinator
package truth.sql

import java.sql.Connection
import com.socrata.datacoordinator.truth.metadata.{DatasetInfo, DatasetMap}

trait TransactionProvider {
  def withReadTransaction[T](datasetId: String)(f: ConnectionInfo => T): T
  def withWriteTransaction[T](datasetId: String)(f: ConnectionInfo => T): T
}

trait ConnectionInfo {
  val datasetMap: DatasetMap
  val datasetInfo: Option[DatasetInfo]
  val connection: Connection
}
