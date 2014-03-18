package com.socrata.datacoordinator.truth.loader.sql.messages

import com.socrata.datacoordinator.truth.metadata
import com.google.protobuf.ByteString
import com.socrata.datacoordinator.id.{UserColumnId, DatasetId, CopyId, ColumnId}
import org.joda.time.DateTime

object FromProtobuf {
   def convert(ci: UnanchoredColumnInfo): metadata.UnanchoredColumnInfo =
     metadata.UnanchoredColumnInfo(
       systemId = new ColumnId(ci.systemId),
       userColumnId = new UserColumnId(ci.userColumnId),
       typeName = ci.typeName,
       physicalColumnBaseBase = ci.physicalColumnBaseBase,
       isSystemPrimaryKey = ci.isSystemPrimaryKey,
       isUserPrimaryKey = ci.isUserPrimaryKey,
       isVersion = ci.isVersion
     )

   def convert(ci: UnanchoredCopyInfo): metadata.UnanchoredCopyInfo =
     metadata.UnanchoredCopyInfo(
       systemId = new CopyId(ci.systemId),
       copyNumber = ci.copyNumber,
       lifecycleStage = convert(ci.lifecycleStage),
       dataVersion = ci.dataVersion,
       lastModified = convert(ci.lastModified)
     )

  def convert(time: Long): DateTime =
    new DateTime(time)

   def convert(ls: LifecycleStage.EnumVal): metadata.LifecycleStage = ls match {
     case LifecycleStage.Unpublished => metadata.LifecycleStage.Unpublished
     case LifecycleStage.Published => metadata.LifecycleStage.Published
     case LifecycleStage.Snapshotted => metadata.LifecycleStage.Snapshotted
     case LifecycleStage.Discarded => metadata.LifecycleStage.Discarded
     case other => sys.error("Unknown lifecycle stage: " + other.name)
   }

   def convert(di: UnanchoredDatasetInfo): metadata.UnanchoredDatasetInfo =
     metadata.UnanchoredDatasetInfo(
       systemId = new DatasetId(di.systemId),
       nextCounterValue = di.nextCounterValue,
       localeName = di.localeName,
       obfuscationKey = di.obfuscationKey.toByteArray
     )
 }
