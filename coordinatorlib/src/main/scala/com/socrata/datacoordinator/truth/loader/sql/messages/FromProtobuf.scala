package com.socrata.datacoordinator.truth.loader.sql.messages

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.truth.metadata
import com.socrata.datacoordinator.id._
import com.socrata.soql.environment.ColumnName
import org.joda.time.DateTime

object FromProtobuf {
   def convert(ci: LogData.UnanchoredColumnInfo): metadata.UnanchoredColumnInfo =
     metadata.UnanchoredColumnInfo(
       systemId = new ColumnId(ci.systemId),
       userColumnId = new UserColumnId(ci.userColumnId),
       fieldName = ci.fieldName.map(new ColumnName(_)),
       typeName = ci.typeName,
       physicalColumnBaseBase = ci.physicalColumnBaseBase,
       isSystemPrimaryKey = ci.isSystemPrimaryKey,
       isUserPrimaryKey = ci.isUserPrimaryKey,
       isVersion = ci.isVersion,
       computationStrategyInfo = ci.computationStrategyInfo.map(convert(_))
     )

  def convert(ci: LogData.UnanchoredColumnInfo.ComputationStrategyInfo): metadata.ComputationStrategyInfo =
    metadata.ComputationStrategyInfo(
      strategyType = new StrategyType(ci.strategyType),
      sourceColumnIds = ci.sourceColumnIds.map(new UserColumnId(_)),
      parameters = JsonUtil.parseJson[JObject](ci.parameters).fold(err => sys.error(err.english), identity)
    )

   def convert(ci: LogData.UnanchoredCopyInfo): metadata.UnanchoredCopyInfo =
     metadata.UnanchoredCopyInfo(
       systemId = new CopyId(ci.systemId),
       copyNumber = ci.copyNumber,
       lifecycleStage = convert(ci.lifecycleStage),
       dataVersion = ci.dataVersion,
       lastModified = convert(ci.lastModified)
     )

  def convert(time: Long): DateTime =
    new DateTime(time)

   def convert(ls: LogData.LifecycleStage): metadata.LifecycleStage = ls match {
     case LogData.LifecycleStage.Unpublished => metadata.LifecycleStage.Unpublished
     case LogData.LifecycleStage.Published => metadata.LifecycleStage.Published
     case LogData.LifecycleStage.Snapshotted => metadata.LifecycleStage.Snapshotted
     case LogData.LifecycleStage.Discarded => metadata.LifecycleStage.Discarded
     case LogData.LifecycleStage.Unrecognized(other) => sys.error("Unknown lifecycle stage: " + other)
   }

   def convert(di: LogData.UnanchoredDatasetInfo): metadata.UnanchoredDatasetInfo =
     metadata.UnanchoredDatasetInfo(
       systemId = new DatasetId(di.systemId),
       nextCounterValue = di.nextCounterValue,
       localeName = di.localeName,
       obfuscationKey = di.obfuscationKey.toByteArray,
       resourceName = di.resourceName
     )

  def convert(ri: LogData.UnanchoredRollupInfo): metadata.UnanchoredRollupInfo =
    metadata.UnanchoredRollupInfo(
      name = new RollupName(ri.name),
      soql = ri.soql
    )
 }
