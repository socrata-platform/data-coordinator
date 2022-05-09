package com.socrata.datacoordinator
package truth.loader

import com.rojoma.json.v3.ast.JObject
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, ComputationStrategyInfo, CopyInfo, IndexInfo, RollupInfo}
import com.socrata.datacoordinator.id.{IndexName, RowId}
import com.socrata.soql.environment.ColumnName
import org.joda.time.DateTime

class NullLogger[CT, CV] extends Logger[CT, CV] {
  def columnCreated(info: ColumnInfo[CT]): Unit = {}

  def columnRemoved(info: ColumnInfo[CT]): Unit = {}

  def rowIdentifierSet(info: ColumnInfo[CT]): Unit = {}

  def rowIdentifierCleared(info: ColumnInfo[CT]): Unit = {}

  def systemIdColumnSet(info: ColumnInfo[CT]): Unit = {}

  def workingCopyCreated(info: CopyInfo): Unit = {}

  def dataCopied(): Unit = {}

  def lastModifiedChanged(lastModified: DateTime): Unit = {}

  def snapshotDropped(info: CopyInfo): Unit = {}

  def workingCopyDropped(): Unit = {}

  def workingCopyPublished(): Unit = {}

  def rollupCreatedOrUpdated(info: RollupInfo): Unit = {}

  def rollupDropped(info: RollupInfo): Unit = {}

  def secondaryReindex() = {}

  def indexDirectiveCreatedOrUpdated(info: ColumnInfo[CT], directive: JObject): Unit = {}

  def indexDirectiveDropped(info: ColumnInfo[CT]): Unit = {}

  def indexCreatedOrUpdated(info: IndexInfo): Unit = {}

  def indexDropped(name: IndexName): Unit = {}

  def endTransaction() = None

  def insert(systemID: RowId, row: Row[CV]): Unit = {}

  def update(sid: RowId, oldRow: Option[Row[CV]], newRow: Row[CV]): Unit = {}

  def delete(systemID: RowId, oldRow: Option[Row[CV]]): Unit = {}

  def counterUpdated(nextCtr: Long): Unit = {}

  def close(): Unit = {}

  def truncated(): Unit = {}

  def versionColumnSet(info: ColumnInfo[CT]): Unit = {}

  def computationStrategyCreated(info: ColumnInfo[CT], cs: ComputationStrategyInfo): Unit = {}

  def computationStrategyRemoved(info: ColumnInfo[CT]): Unit = {}

  def fieldNameUpdated(info: ColumnInfo[CT]): Unit = {}
}

object NullLogger extends NullLogger[Any, Any] {
  def apply[A, B] = this.asInstanceOf[NullLogger[A, B]]
}
