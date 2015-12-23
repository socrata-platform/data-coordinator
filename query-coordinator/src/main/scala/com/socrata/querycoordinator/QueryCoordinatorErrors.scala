package com.socrata.querycoordinator



object QueryCoordinatorErrors {
  sealed abstract class QCErrors

  object SoqlQueryErrors extends QCErrors {
    val aggregateInUngroupedContext =  "query.soql.aggregate-in-ungrouped-context"
    val columnNotInGroupBys = "query.soql.column-not-in-group-bys"
    val repeatedException = "query.soql.repeated-exception"
    val duplicateAlias = "query.soql.duplicate-alias"
    val noSuchColumn = "query.soql.no-such-column"
    val circularAliasDefinition = "query.soql.circular-alias-definition"
    val unexpectedEscape = "query.soql.unexpected-escape"
    val badUnicodeEscapeCharacter = "query.soql.bad-unicode-escape-character"
    val unicodeCharacterOutOfRange = "query.soql.unicode-character-out-of-range"
    val unexpectedCharacter = "query.soql.unexpected-character"
    val unexpectedEOF = "query.soql.unexpected-eof"
    val unterminatedString = "query.soql.unterminated-string"
    val badParse = "query.soql.bad-parse"
    val noSuchFunction = "query.soql.no-such-function"
    val typeMismatch = "query.soql.type-mismatch"
    val ambiguousCall = "query.soql.ambiguous-call"
    val nonBooleanHaving = "query.soql.non-boolean-having"
    val nonBooleanWhere = "query.soql.non-boolean-where"
    val nonGroupableGroup = "query.soql.non-groupable-group"
    val unOrderableOrder = "query.soql.un-orderable-order"
  }

  object QueryErrors extends QCErrors {
    val datasourceUnavailable = "query.datasource.unavailable"
    val doesNotExist = "query.dataset.does-not-exist"
  }

  object RequestErrors extends QCErrors {
    val noDatasetSpecified = "req.no-dataset-specified"
    val noQuerySpecified = "req.no-query-specified"
    val unknownColumnIds = "req.unknown.column-ids"
    val rowLimitExceeded = "req.row-limit-exceeded"
  }

}
