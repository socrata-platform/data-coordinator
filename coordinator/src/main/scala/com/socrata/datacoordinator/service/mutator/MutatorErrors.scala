package com.socrata.datacoordinator.service.mutator

import com.rojoma.json.ast.{JObject, JValue}
import com.socrata.datacoordinator.id.{RowVersion, UserColumnId, DatasetId}
import com.socrata.datacoordinator.truth.metadata.{LifecycleStage, Schema}
import com.socrata.soql.environment.TypeName
import com.socrata.datacoordinator.truth.loader.Failure

sealed abstract class MutationException(msg: String = null, cause: Throwable = null) extends Exception(msg, cause)

sealed abstract class CommonMutationException extends MutationException
object CommonMutationException {
  case class CannotAcquireDatasetWriteLock(name: DatasetId) extends CommonMutationException
  case class SystemInReadOnlyMode() extends CommonMutationException
}

sealed abstract class MutationScriptHeaderException extends MutationException
object MutationScriptHeaderException {
  sealed trait Create extends MutationScriptHeaderException
  sealed trait Copy extends MutationScriptHeaderException
  sealed trait DDL extends MutationScriptHeaderException
  sealed trait Upsert extends MutationScriptHeaderException
  sealed trait Common extends Create with Copy with DDL with Upsert
  case class EmptyCommandStream() extends MutationScriptHeaderException with Create with DDL with Upsert
  case class InvalidLocale(locale: String) extends MutationScriptHeaderException with Create
  case class NoSuchDataset(name: DatasetId) extends MutationScriptHeaderException with Copy with DDL with Upsert
  case class IncorrectLifecycleStage(name: DatasetId, currentLifecycleStage: LifecycleStage, expected: Set[LifecycleStage]) extends MutationScriptHeaderException with DDL with Copy
  case class InitialCopyDrop(name: DatasetId) extends MutationScriptHeaderException with Copy
  case class MismatchedSchemaHash(name: DatasetId, schema: Schema) extends MutationScriptHeaderException with Copy with DDL with Upsert
  case class MissingHeaderField(obj: JObject, field: String) extends MutationScriptHeaderException with Common
  case class InvalidHeaderFieldValue(obj: JObject, field: String, value: JValue) extends MutationScriptHeaderException with Common
  case class HeaderIsNotAnObject(value: JValue) extends MutationScriptHeaderException with Common
}

sealed abstract class DDLException extends MutationException {
  val index: Long
}
object DDLException {
  case class MissingCommandField(obj: JObject, field: String, index: Long) extends DDLException
  case class InvalidCommandFieldValue(obj: JObject, field: String, value: JValue, index: Long) extends DDLException
  case class CommandIsNotAnObject(value: JValue, index: Long) extends DDLException
  case class NoSuchColumn(dataset: DatasetId, id: UserColumnId, index: Long) extends DDLException
  case class NoSuchColumnLabel(label: String, index: Long) extends DDLException
  case class NoSuchType(name: TypeName, index: Long) extends DDLException
  case class PrimaryKeyAlreadyExists(dataset: DatasetId, id: UserColumnId, existingName: UserColumnId, index: Long) extends DDLException
  case class InvalidTypeForPrimaryKey(dataset: DatasetId, name: UserColumnId, typ: TypeName, index: Long) extends DDLException
  case class NullsInColumn(dataset: DatasetId, id: UserColumnId, index: Long) extends DDLException
  case class NotPrimaryKey(dataset: DatasetId, id: UserColumnId, index: Long) extends DDLException
  case class DuplicateValuesInColumn(dataset: DatasetId, id: UserColumnId, index: Long) extends DDLException
  case class InvalidSystemColumnOperation(dataset: DatasetId, id: UserColumnId, op: String, index: Long) extends DDLException
  case class DeleteRowIdentifierNotAllowed(dataset: DatasetId, id: UserColumnId, index: Long) extends DDLException
}

sealed abstract class UpsertException extends MutationException {
  val index: Long
}
object UpsertException {
  case class InvalidUpsertCommand(dataset: DatasetId, value: JValue, index: Long) extends UpsertException
  case class InvalidValue(dataset: DatasetId, column: UserColumnId, typ: TypeName, value: JValue, index: Long) extends UpsertException
  case class UnknownColumnId(dataset: DatasetId, column: UserColumnId, index: Long) extends UpsertException
  case class UpsertError(dataset: DatasetId, failure: Failure[JValue], versionToJson: RowVersion => JValue, index: Long) extends UpsertException
}
