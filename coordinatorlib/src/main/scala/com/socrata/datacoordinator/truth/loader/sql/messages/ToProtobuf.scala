package com.socrata.datacoordinator.truth.loader.sql.messages

import com.socrata.datacoordinator.truth.metadata
import com.google.protobuf.ByteString

object ToProtobuf {
  def convert(ci: metadata.UnanchoredColumnInfo): UnanchoredColumnInfo =
    UnanchoredColumnInfo(
      systemId = ci.systemId.underlying,
      userColumnId = ci.userColumnId.underlying,
      typeName = ci.typeName,
      physicalColumnBaseBase = ci.physicalColumnBaseBase,
      isSystemPrimaryKey = ci.isSystemPrimaryKey,
      isUserPrimaryKey = ci.isUserPrimaryKey,
      isVersion = ci.isVersion
    )

  def convert(ci: metadata.UnanchoredCopyInfo): UnanchoredCopyInfo =
    UnanchoredCopyInfo(
      systemId = ci.systemId.underlying,
      copyNumber = ci.copyNumber,
      lifecycleStage = convert(ci.lifecycleStage),
      dataVersion = ci.dataVersion
    )

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
}
