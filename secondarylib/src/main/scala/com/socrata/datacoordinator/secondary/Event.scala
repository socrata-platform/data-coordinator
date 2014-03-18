package com.socrata.datacoordinator.secondary

import org.joda.time.DateTime

sealed abstract class Event[+CT, +CV] extends Product

case object Truncated extends Event[Nothing, Nothing]
case class ColumnCreated[CT](info: ColumnInfo[CT]) extends Event[CT, Nothing]
case class ColumnRemoved[CT](info: ColumnInfo[CT]) extends Event[CT, Nothing]
case class RowIdentifierSet[CT](info: ColumnInfo[CT]) extends Event[CT, Nothing]
case class RowIdentifierCleared[CT](info: ColumnInfo[CT]) extends Event[CT, Nothing]
case class SystemRowIdentifierChanged[CT](info: ColumnInfo[CT]) extends Event[CT, Nothing]
case class VersionColumnChanged[CT](info: ColumnInfo[CT]) extends Event[CT, Nothing]
case class LastModifiedChanged[CT](lastModified: DateTime) extends Event[CT, Nothing]
case class WorkingCopyCreated(copyInfo: CopyInfo) extends Event[Nothing, Nothing]
case object WorkingCopyDropped extends Event[Nothing, Nothing]
case object DataCopied extends Event[Nothing, Nothing]
case class SnapshotDropped(info: CopyInfo) extends Event[Nothing, Nothing]
case object WorkingCopyPublished extends Event[Nothing, Nothing]
case class RowDataUpdated[CV](operations: Seq[Operation[CV]]) extends Event[Nothing, CV]
