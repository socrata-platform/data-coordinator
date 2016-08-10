package com.socrata.datacoordinator.external

object ThreadsMutationError{
  val MAXED_OUT = "mutation.threads.maxed-out"
}

object DatasetCreateError{
  val INVALID_LOCALE = "create.dataset.invalid-locale"
}

object RollupDeleteError{
  val DOES_NOT_EXIST = "delete.rollup.does-not-exist"
}

// Request Errors

object RequestError{
  val PRECONDITION_FAILED = "req.precondition-failed"
  val UNHANDLED_ERROR = "req.unhandled-error-response"
}

object ContentTypeRequestError{
  val BAD_REQUEST = "req.content-type.request-error"
  val MISSING = "req.content-type.missing"
  val UNPARSABLE = "req.content-type.unparsable"
  val NOT_JSON = "req.content-type.not-json"
  val UNKNOWN_CHARSET = "req.content-type.unknown-charset"
}

object ExportRequestError{
  val MISMATCHED_SCHEMA = "req.export.mismatched-schema"
  val INVALID_ROW_ID = "req.export.invalid-row-id"
  val UNKNOWN_COLUMNS = "req.export.unknown-columns"
}

object BodyRequestError{
  val COMMAND_TOO_LARGE = "req.body.command-too-large"
  val MALFORMED_JSON = "req.body.malformed-json"
  val NOT_JSON_ARRAY = "req.body.not-json-array"
}

object ScriptHeaderRequestError{
  val MISMATCHED_SCHEMA = "req.script.header.mismatched-schema"
  val MISSING = "req.script.header.missing"
}

object ScriptCommandRequestError{
  val NON_OBJECT = "req.script.command.non-object"
  val MISSING_FIELD = "req.script.command.missing-field"
  val INVALID_FIELD = "req.script.command.invalid-field"
  val UNKNOWN_LABEL = "req.script.command.unknown-label"
}

// Update Errors

object UpdateError{
  val READ_ONLY_MODE = "update.read-only-mode"
  val TYPE_UNKNOWN = "update.type.unknown"
  val ROW_VERSION_MISMATCH = "update.row-version-mismatch"
  val VERSION_ON_NEW_ROW = "update.version-on-new-row"
  val INSERT_IN_UPDATE_ONLY = "update.insert-in-update-only"
}


object ColumnUpdateError{
  val EXISTS = "update.column.exists"
  val ILLEGAL_ID = "update.column.illegal-id"
  val SYSTEM = "update.column.system"
  val NOT_FOUND = "update.column.not-found"
}

object DatasetUpdateError{
  val DOES_NOT_EXIST = "update.dataset.does-not-exist"
  val SNAPSHOT_DOES_NOT_EXIST = "update.snapshot.does-not-exist"
  val SECONDARIES_OUT_OF_DATE = "update.dataset.feedback-in-progress"
  val TEMP_NOT_WRITABLE = "update.dataset.temporarily-not-writable"
  val INVALID_STATE = "update.dataset.invalid-state"
  val INITIAL_COPY_DROP = "update.dataset.initial-copy-drop"
  val OPERATION_AFTER_DROP = "update.dataset.operation-after-drop"
  val LACKS_PRIMARY_KEY = "update.dataset.lacks-primary-key"
}

object RowUpdateError{
  val PRIMARY_KEY_NONEXISTENT_OR_NULL = "update.row.primary-key-nonexistent-or-null"
  val NO_SUCH_ID = "update.row.no-such-id"
  val UNPARSABLE_VALUE = "update.row.unparsable-value"
  val UNKNOWN_COLUMN = "update.row.unknown-column"
}

object ScriptRowDataUpdateError{
  val INVALID_VALUE = "update.script.row-data.invalid-value"
}

object RowIdentifierUpdateError{
  val ALREADY_SET = "update.row-identifier.already-set"
  val INVALID_TYPE = "update.row-identifier.invalid-type"
  val DUPLICATE_VALUES = "update.row-identifier.duplicate-values"
  val NULL_VALUES = "update.row-identifier.null-values"
  val NOT_ROW_IDENTIFIER = "update.row-identifier.not-row-identifier"
  val DELETE = "update.row-identifier.delete"
}
