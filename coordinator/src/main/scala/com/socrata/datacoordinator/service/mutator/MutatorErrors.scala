package com.socrata.datacoordinator.service.mutator

import com.rojoma.json.ast.{JObject, JValue}
import com.socrata.datacoordinator.id.{RowVersion, UserColumnId, DatasetId}
import com.socrata.datacoordinator.truth.metadata.{LifecycleStage, Schema}
import com.socrata.soql.environment.TypeName
import com.socrata.datacoordinator.truth.loader.Failure

sealed abstract class MutationException(msg: String = null, cause: Throwable = null) extends Exception(msg, cause)

case class CannotAcquireDatasetWriteLock(name: DatasetId) extends MutationException
case class SystemInReadOnlyMode() extends MutationException

sealed abstract class MutationScriptHeaderException extends MutationException
sealed trait CreateDatasetHeaderException
sealed trait CopyHeaderException
sealed trait DDLHeaderException
sealed trait UpsertHeaderException
sealed trait CommonHeaderException extends CreateDatasetHeaderException with CopyHeaderException with DDLHeaderException with UpsertHeaderException
case class EmptyCommandStream() extends MutationScriptHeaderException with CreateDatasetHeaderException with DDLHeaderException with UpsertHeaderException
case class InvalidLocale(locale: String) extends MutationScriptHeaderException with CreateDatasetHeaderException
case class NoSuchDataset(name: DatasetId) extends MutationScriptHeaderException with CopyHeaderException with DDLHeaderException with UpsertHeaderException
case class IncorrectLifecycleStage(name: DatasetId, currentLifecycleStage: LifecycleStage, expected: Set[LifecycleStage]) extends MutationScriptHeaderException with DDLHeaderException with CopyHeaderException
case class InitialCopyDrop(name: DatasetId) extends MutationScriptHeaderException with CopyHeaderException
case class MismatchedSchemaHash(name: DatasetId, schema: Schema) extends MutationScriptHeaderException with CopyHeaderException with DDLHeaderException with UpsertHeaderException
case class MissingHeaderField(obj: JObject, field: String) extends MutationScriptHeaderException with CommonHeaderException
case class InvalidHeaderFieldValue(obj: JObject, field: String, value: JValue) extends MutationScriptHeaderException with CommonHeaderException
case class HeaderIsNotAnObject(value: JValue) extends MutationScriptHeaderException with CommonHeaderException

sealed abstract class DDLException extends MutationException {
  val index: Long
}
case class MissingCommandField(obj: JObject, field: String, index: Long) extends DDLException
case class InvalidCommandFieldValue(obj: JObject, field: String, value: JValue, index: Long) extends DDLException
case class CommandIsNotAnObject(value: JValue, index: Long) extends DDLException
case class NoSuchColumn(dataset: DatasetId, id: UserColumnId, index: Long) extends DDLException
case class NoSuchColumnLabel(dataset: DatasetId, label: String, index: Long) extends DDLException
case class NoSuchType(name: TypeName, index: Long) extends DDLException
case class PrimaryKeyAlreadyExists(dataset: DatasetId, id: UserColumnId, existingName: UserColumnId, index: Long) extends DDLException
case class InvalidTypeForPrimaryKey(dataset: DatasetId, name: UserColumnId, typ: TypeName, index: Long) extends DDLException
case class NullsInColumn(dataset: DatasetId, id: UserColumnId, index: Long) extends DDLException
case class NotPrimaryKey(dataset: DatasetId, id: UserColumnId, index: Long) extends DDLException
case class DuplicateValuesInColumn(dataset: DatasetId, id: UserColumnId, index: Long) extends DDLException
case class InvalidSystemColumnOperation(dataset: DatasetId, id: UserColumnId, op: String, index: Long) extends DDLException
case class DeleteRowIdentifierNotAllowed(dataset: DatasetId, id: UserColumnId, index: Long) extends DDLException

sealed abstract class RowDataException extends MutationException {
  val index: Long
}
case class InvalidUpsertCommand(dataset: DatasetId, value: JValue, index: Long) extends RowDataException
case class InvalidValue(dataset: DatasetId, column: UserColumnId, typ: TypeName, value: JValue, index: Long) extends RowDataException
case class UnknownColumnId(dataset: DatasetId, column: UserColumnId, index: Long) extends RowDataException
case class UpsertError(dataset: DatasetId, failure: Failure[JValue], versionToJson: RowVersion => JValue, index: Long) extends RowDataException
