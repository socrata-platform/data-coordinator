package com.socrata.datacoordinator.secondary

import com.rojoma.json.v3.ast.JObject
import com.socrata.soql.environment.ColumnName
import org.joda.time.DateTime

sealed abstract class Event[+CT, +CV] extends Product

case object Truncated extends Event[Nothing, Nothing]
case class ColumnCreated[CT](info: ColumnInfo[CT]) extends Event[CT, Nothing]
case class ColumnRemoved[CT](info: ColumnInfo[CT]) extends Event[CT, Nothing]
case class ComputationStrategyCreated[CT](info: ColumnInfo[CT]) extends Event[CT, Nothing]
case class ComputationStrategyRemoved[CT](info: ColumnInfo[CT]) extends Event[CT, Nothing]
case class FieldNameUpdated[CT](info: ColumnInfo[CT]) extends Event[CT, Nothing]
case class RowIdentifierSet[CT](info: ColumnInfo[CT]) extends Event[CT, Nothing]
case class RowIdentifierCleared[CT](info: ColumnInfo[CT]) extends Event[CT, Nothing]
case class SystemRowIdentifierChanged[CT](info: ColumnInfo[CT]) extends Event[CT, Nothing]
case class VersionColumnChanged[CT](info: ColumnInfo[CT]) extends Event[CT, Nothing]
case class LastModifiedChanged[CT](lastModified: DateTime) extends Event[CT, Nothing]
case class WorkingCopyCreated(copyInfo: CopyInfo) extends Event[Nothing, Nothing]
case object WorkingCopyDropped extends Event[Nothing, Nothing]
case object DataCopied extends Event[Nothing, Nothing]
case class SnapshotDropped(info: CopyInfo) extends Event[Nothing, Nothing] // This will never occur!  Remove me the next time you break binary compat!
case class RollupCreatedOrUpdated(info: RollupInfo) extends Event[Nothing, Nothing]
case class RollupDropped(info: RollupInfo) extends Event[Nothing, Nothing]
case class RowsChangedPreview(rowsInserted: Long, rowsUpdated: Long, rowsDeleted: Long, truncated: Boolean) extends Event[Nothing, Nothing]
case object WorkingCopyPublished extends Event[Nothing, Nothing]
case class RowDataUpdated[CV](operations: Seq[Operation[CV]]) extends Event[Nothing, CV]
case object SecondaryReindex extends Event[Nothing, Nothing]
case class IndexDirectiveCreatedOrUpdated[CT](info: ColumnInfo[CT], directive: JObject) extends Event[CT, Nothing]
case class IndexDirectiveDropped[CT](info: ColumnInfo[CT]) extends Event[CT, Nothing]
