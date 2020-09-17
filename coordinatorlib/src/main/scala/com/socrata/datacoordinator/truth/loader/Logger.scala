package com.socrata.datacoordinator
package truth.loader

import com.rojoma.json.v3.ast.JObject
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, ComputationStrategyInfo, CopyInfo, RollupInfo}
import com.socrata.soql.environment.ColumnName
import org.joda.time.DateTime

trait Logger[CT, CV] extends DataLogger[CV] {
  def truncated(): Unit
  def columnCreated(info: ColumnInfo[CT]): Unit
  def columnRemoved(info: ColumnInfo[CT]): Unit
  def computationStrategyCreated(info: ColumnInfo[CT], computationStrategyInfo: ComputationStrategyInfo): Unit
  def computationStrategyRemoved(info: ColumnInfo[CT]): Unit
  def fieldNameUpdated(info: ColumnInfo[CT]): Unit
  def rowIdentifierSet(newIdentifier: ColumnInfo[CT]): Unit
  def rowIdentifierCleared(oldIdentifier: ColumnInfo[CT]): Unit
  def systemIdColumnSet(info: ColumnInfo[CT]): Unit
  def lastModifiedChanged(time: DateTime): Unit
  def versionColumnSet(info: ColumnInfo[CT]): Unit
  def workingCopyCreated(info: CopyInfo): Unit
  def dataCopied(): Unit
  def workingCopyDropped(): Unit
  def snapshotDropped(info: CopyInfo): Unit
  def workingCopyPublished(): Unit
  def rollupCreatedOrUpdated(info: RollupInfo): Unit
  def rollupDropped(info: RollupInfo): Unit
  def secondaryReindex(): Unit
  def secondaryAddIndex(fieldName: ColumnName, directives: JObject): Unit
  def secondaryDeleteIndex(fieldName: ColumnName): Unit

  /** Logs the end of the transaction and returns its version number.
   * @return The new log version number, or None if no other method was called. */
  def endTransaction(): Option[Long]
}
