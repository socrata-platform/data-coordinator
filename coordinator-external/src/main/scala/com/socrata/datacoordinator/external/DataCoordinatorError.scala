package com.socrata.datacoordinator.external

sealed class DataCoordinatorError

class ThreadsMutationError extends DataCoordinatorError
object ThreadsMutationError{
  val MAXED_OUT = "mutation.threads.maxed-out"
}

class DatasetCreateError extends DataCoordinatorError
object DatasetCreateError{
  val INVALID_LOCALE = "create.dataset.invalid-locale"
}

class RollupDeleteError extends DataCoordinatorError
object RollupDeleteError{
  val DOES_NOT_EXIST = "delete.rollup.does-not-exist"
}

// Request Errors

class RequestError extends DataCoordinatorError
object RequestError{
  val PRECONDITION_FAILED = "req.precondition-failed"
  val UNHANDLED_ERROR = "req.unhandled-error-response"
}

class ContentTypeRequestError extends RequestError
object ContentTypeRequestError{
  val BAD_REQUEST = "req.content-type.request-error"
  val MISSING = "req.content-type.missing"
  val UNPARSABLE = "req.content-type.unparsable"
  val NOT_JSON = "req.content-type.not-json"
  val UNKNOWN_CHARSET = "req.content-type.unknown-charset"
}

class ExportRequestError extends RequestError
object ExportRequestError{
  val MISMATCHED_SCHEMA = "req.export.mismatched-schema"
  val INVALID_ROW_ID = "req.export.invalid-row-id"
}

class BodyRequestError extends RequestError
object BodyRequestError{
  val COMMAND_TOO_LARGE = "req.body.command-too-large"
  val MALFORMED_JSON = "req.body.malformed-json"
  val NOT_JSON_ARRAY = "req.body.not-json-array"
}

class ScriptHeaderRequestError extends RequestError
object ScriptHeaderRequestError{
  val MISMATCHED_SCHEMA = "req.script.header.mismatched-schema"
  val MISSING = "req.script.header.missing"
}

class ScriptCommandRequestError extends RequestError
object ScriptCommandRequestError{
  val NON_OBJECT = "req.script.command.non-object"
  val MISSING_FIELD = "req.script.command.missing-field"
  val INVALID_FIELD = "req.script.command.invalid-field"
  val UNKNOWN_LABEL = "req.script.command.unknown-label"
}

// Update Errors

class UpdateError extends DataCoordinatorError
object UpdateError{
  val READ_ONLY_MODE = "update.read-only-mode"
  val TYPE_UNKNOWN = "update.type.unknown"
  val ROW_VERSION_MISMATCH = "update.row-version-mismatch"
  val VERSION_ON_NEW_ROW = "update.version-on-new-row"
}


class ColumnUpdateError extends UpdateError
object ColumnUpdateError{
  val EXISTS = "update.column.exists"
  val ILLEGAL_ID = "update.column.illegal-id"
  val SYSTEM = "update.column.system"
  val NOT_FOUND = "update.column.not-found"
  val NO_COMP = "update.column.no-computation-strategy"
}

class DatasetUpdateError extends UpdateError
object DatasetUpdateError{
  val DOES_NOT_EXIST = "update.dataset.does-not-exist"
  val TEMP_NOT_WRITABLE = "update.dataset.temporarily-not-writable"
  val INVALID_STATE = "update.dataset.invalid-state"
  val INITIAL_COPY_DROP = "update.dataset.initial-copy-drop"
  val OPERATION_AFTER_DROP = "update.dataset.operation-after-drop"
  val LACKS_PRIMARY_KEY = "update.dataset.lacks-primary-key"
}

class RowUpdateError extends UpdateError
object RowUpdateError{
  val PRIMARY_KEY_NONEXISTENT_OR_NULL = "update.row.primary-key-nonexistent-or-null"
  val NO_SUCH_ID = "update.row.no-such-id"
  val UNPARSABLE_VALUE = "update.row.unparsable-value"
  val UNKNOWN_COLUMN = "update.row.unknown-column"
}

class ScriptRowDataUpdateError extends UpdateError
object ScriptRowDataUpdateError{
  val INVALID_VALUE = "update.script.row-data.invalid-value"
}

class RowIdentifierUpdateError extends UpdateError
object RowIdentifierUpdateError{
  val ALREADY_SET = "update.row-identifier.already-set"
  val INVALID_TYPE = "update.row-identifier.invalid-type"
  val DUPLICATE_VALUES = "update.row-identifier.duplicate-values"
  val NULL_VALUES = "update.row-identifier.null-values"
  val NOT_ROW_IDENTIFIER = "update.row-identifier.not-row-identifier"
  val DELETE = "update.row-identifier.delete"
}