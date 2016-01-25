package com.socrata.datacoordinator.truth.loader.sql.messages

import com.rojoma.json.v3.io.CompactJsonWriter
import com.socrata.datacoordinator.id.ColumnId
import com.socrata.datacoordinator.truth.metadata
import com.google.protobuf.ByteString
import com.socrata.soql.environment.ColumnName
import org.joda.time.DateTime

object ToProtobuf {
  def convert(ci: metadata.UnanchoredColumnInfo): UnanchoredColumnInfo =
    UnanchoredColumnInfo(
      systemId = ci.systemId.underlying,
      userColumnId = ci.userColumnId.underlying,
      typeName = ci.typeName,
      physicalColumnBaseBase = ci.physicalColumnBaseBase,
      isSystemPrimaryKey = ci.isSystemPrimaryKey,
      isUserPrimaryKey = ci.isUserPrimaryKey,
      isVersion = ci.isVersion,
      fieldName = ci.fieldName.map(_.name),
      computationStrategyInfo = ci.computationStrategyInfo.map(convert)
    )

  def convert(ci: metadata.ComputationStrategyInfo): com.socrata.datacoordinator.truth.loader.sql.messages.UnanchoredColumnInfo.ComputationStrategyInfo =
    com.socrata.datacoordinator.truth.loader.sql.messages.UnanchoredColumnInfo.ComputationStrategyInfo(
      strategyType = ci.strategyType.underlying,
      sourceColumnIds = ci.sourceColumnIds.map(_.underlying).to[collection.immutable.Seq],
      parameters = CompactJsonWriter.toString(ci.parameters)
    )

  def convert(ci: metadata.UnanchoredCopyInfo): UnanchoredCopyInfo =
    UnanchoredCopyInfo(
      systemId = ci.systemId.underlying,
      copyNumber = ci.copyNumber,
      lifecycleStage = convert(ci.lifecycleStage),
      dataVersion = ci.dataVersion,
      lastModified = convert(ci.lastModified)
    )

  def convert(dateTime: DateTime): Long =
    dateTime.getMillis

  def convert(ls: metadata.LifecycleStage): LifecycleStage.EnumVal = ls match {
    case metadata.LifecycleStage.Unpublished => LifecycleStage.Unpublished
    case metadata.LifecycleStage.Published => LifecycleStage.Published
    case metadata.LifecycleStage.Snapshotted => LifecycleStage.Snapshotted
    case metadata.LifecycleStage.Discarded => LifecycleStage.Discarded
  }

  def convert(di: metadata.UnanchoredDatasetInfo): UnanchoredDatasetInfo =
    UnanchoredDatasetInfo(
      systemId = di.systemId.underlying,
      nextCounterValue = di.nextCounterValue,
      localeName = di.localeName,
      obfuscationKey = ByteString.copyFrom(di.obfuscationKey)
    )

  def convert(ri: metadata.UnanchoredRollupInfo): UnanchoredRollupInfo =
    UnanchoredRollupInfo(
      name = ri.name.underlying,
      soql = ri.soql
    )

  def convert(columnName: ColumnName): String =
    columnName.name

  def convert(columnId: ColumnId): Long =
    columnId.underlying
}
