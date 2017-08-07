package com.socrata.datacoordinator.secondary.feedback

import com.socrata.datacoordinator.id.UserColumnId

sealed abstract class RequestFailure {
  def english: String
}

case object FailedToDiscoverDataCoordinator extends RequestFailure {
  val english = "Failed to discover data-coordinator host and port"
}

case object DataCoordinatorBusy extends RequestFailure {
  val english = "Data-coordinator responded with 409"
}

case object DatasetDoesNotExist extends RequestFailure {
  val english = "Dataset does not exist"
}

case class UnexpectedError(reason: String, cause: Throwable) extends RequestFailure {
  val english = s"Unexpected error in data-coordinator client: $reason"
}

sealed abstract class SchemaFailure {
  def english: String
}

sealed abstract class UpdateSchemaFailure extends SchemaFailure

case class PrimaryKeyColumnDoesNotExist(id: UserColumnId) extends UpdateSchemaFailure {
  val english = s"Primary key column ${id.underlying} does not exist"
}

case class TargetColumnDoesNotExist(id: UserColumnId) extends UpdateSchemaFailure {
  val english = s"Target column ${id.underlying} does not exist"
}

case object PrimaryKeyColumnHasChanged extends UpdateSchemaFailure {
  val english = "The primary key column has changed"
}

case class ColumnsDoNotExist(columns: Set[UserColumnId]) extends SchemaFailure {
  val english = s"Columns ${columns.map(_.underlying)} do not exist"
}
