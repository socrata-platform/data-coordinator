package com.socrata.querycoordinator.resources

import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.ast.{JNumber, JObject, JString, JValue}
import com.rojoma.json.v3.codec.JsonEncode
import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.querycoordinator.QueryCoordinatorErrors
import com.socrata.querycoordinator.QueryCoordinatorErrors._
import com.socrata.soql.exceptions._
import com.socrata.http.server.implicits._
import QueryCoordinatorErrors.SoqlQueryErrors._

import scala.util.control.ControlThrowable

trait QueryService {

  val log = org.slf4j.LoggerFactory.getLogger(classOf[QueryService])

  val responseBuffer = 4096
  val headerSocrataResource = "X-Socrata-Resource"
  val unexpectedError = "Unexpected response when fetching schema from secondary: {}"

  val qpHyphen = "-"
  val qpFunction = "function"
  val qpColumn = "column"
  val qpRow = "row"
  val qpName = "name"
  val qpChar = "character"
  val qpData = "data"
  val qpDataset = "dataset"
  val qpLimit = "limit"
  val qpPosition = "position"
  val qpLine = "line"
  val qpObfuscateId = "obfuscateId"

  def soqlError(e: SoQLException): // scalastyle:ignore cyclomatic.complexity method.length
  (String, String, Map[String, JValue], Map[String, JValue]) = {
    val position = Map(
      qpRow -> JNumber(e.position.line),
      qpColumn -> JNumber(e.position.column),
      qpLine -> JString(e.position.longString)
    )

    e match {
     case AggregateInUngroupedContext(func, clause, _) =>
       (aggregateInUngroupedContext,
        s"Aggregate function '${func.name}' used in ungrouped context: ${clause}; position: $position",
        Map(qpFunction -> JString(func.name), "clause" -> JString(clause)), position)
     case ColumnNotInGroupBys(column, _) =>
       (columnNotInGroupBys, s"Column '${column.name}' is not in group by; position: $position",
        Map(qpColumn -> JString(column.name)), position)
     case RepeatedException(column, _) =>
       (repeatedException, s"Repeated exception with column: ${column.name}; position: $position",
        Map(qpColumn -> JString(column.name)), position)
     case DuplicateAlias(name, _) =>
       (duplicateAlias, s"Duplicate alias: ${name.name}; position: $position",
        Map(qpName -> JString(name.name)), position)
     case NoSuchColumn(column, _) =>
       (noSuchColumn, s"No such column: ${column.name}; position: $position",
        Map(qpColumn -> JString(column.name)), position)
     case CircularAliasDefinition(name, _) =>
       (circularAliasDefinition, s"Circular alias definition: ${name.name}; position: $position",
        Map(qpName -> JString(name.name)), position)
     case UnexpectedEscape(c, _) =>
       (unexpectedEscape, s"Unexpected escape: ${c.toString}; position: $position",
        Map(qpChar -> JString(c.toString)), position)
     case BadUnicodeEscapeCharacter(c, _) =>
       (badUnicodeEscapeCharacter, s"Bad unicode escape character: ${c.toString}; position: $position",
        Map(qpChar -> JString(c.toString)), position)
     case UnicodeCharacterOutOfRange(x, _) =>
       (unicodeCharacterOutOfRange, s"Unicode character out of range: $x; position: $position",
        Map("number" -> JNumber(x)), position)
     case UnexpectedCharacter(c, _) =>
       (unexpectedCharacter, s"Unexpected character: ${c.toString}; position: $position",
        Map(qpChar -> JString(c.toString)), position)
     case UnexpectedEOF(_) =>
       (unexpectedEOF, s"Unexpected EOF; position: $position",
        Map.empty, position)
     case UnterminatedString(_) =>
       (unterminatedString, s"Unterminated string; position: $position",
        Map.empty, position)
     case BadParse(msg, _) =>
       // TODO: this needs to be machine-readable
       (badParse, s"Bad parse: $msg; position: $position",
        Map("message" -> JString(msg)), position)
     case NoSuchFunction(name, arity, _) =>
       (noSuchFunction, s"No such function '${name.name}'; arity=$arity; position: $position",
        Map(qpFunction -> JString(name.name), "arity" -> JNumber(arity)), position)
     case TypeMismatch(name, actual, _) =>
       (typeMismatch, s"Type mismatch for ${name.name}, is ${actual.name}; position: $position",
        Map(qpFunction -> JString(name.name), "type" -> JString(actual.name)), position)
     case AmbiguousCall(name, _) =>
       (ambiguousCall, s"Ambiguous call of function ${name.name}; position: $position",
        Map(qpFunction -> JString(name.name)), position)
     case NonBooleanHaving(_, pos) =>
       (nonBooleanHaving, "Non-boolean having", Map.empty, position)
     case NonBooleanWhere(_, _) =>
       (nonBooleanWhere, "Non-boolean where", Map.empty, position)
     case NonGroupableGroupBy(_, _) =>
       (nonGroupableGroup, "Non-groupable group by", Map.empty, position)
     case UnorderableOrderBy(_, _) =>
       (unOrderableOrder, "Un-orderable order by", Map.empty, position)
    }
  }

  def soqlErrorResponse(dataset: String, e: SoQLException): HttpResponse = {
    val (errCode, errMsg, errData, position) = soqlError(e)
    val data = errData ++ Map(qpDataset -> JString(dataset), qpPosition -> JObject(position))
    BadRequest ~> errContent(errCode, errMsg, data)
  }

  def errContent(errCode: String, msg: String, data: Map[String, JValue]): HttpResponse = {
    val error = JObject(Map("errorCode" -> JString(errCode),
                            "description" -> JString(msg),
                            qpData -> JObject(data.toMap)))
    log.info(error.toString)
    Json(error)
  }

  case class FragmentedQuery(select: Option[String],
                             where: Option[String],
                             group: Option[String],
                             having: Option[String],
                             search: Option[String],
                             order: Option[String],
                             limit: Option[String],
                             offset: Option[String])

  case class FinishRequest(response: HttpResponse) extends ControlThrowable


  def finishRequest(response: HttpResponse): Nothing = {
    throw new FinishRequest(response)
  }

  def noSecondaryAvailable(dataset: String): HttpServletResponse => Unit = {
    ServiceUnavailable ~> errContent(QueryErrors.datasourceUnavailable, s"Dataset $dataset not available",
                                     Map(qpDataset -> JString(dataset)))
  }

  def internalServerError: HttpResponse = {
    InternalServerError ~> Json("Internal server error")
  }

  def notFoundResponse(dataset: String): HttpServletResponse => Unit = {
    NotFound ~> errContent(QueryErrors.doesNotExist, s"Dataset $dataset not found",
                           Map(qpDataset -> JString(dataset)))
  }

  def noDatasetResponse: HttpResponse = {
    BadRequest ~> errContent(RequestErrors.noDatasetSpecified, "No dataset specified", Map())
  }

  def noQueryResponse: HttpResponse = {
    BadRequest ~> errContent(RequestErrors.noQuerySpecified, "No query specified", Map())
  }

  def unknownColumnIds(columnIds: Set[String]): HttpResponse = {
    BadRequest ~> errContent(RequestErrors.unknownColumnIds, s"Columns $columnIds are unknown",
                             Map("columns" -> JsonEncode.toJValue(columnIds.toSeq)))
  }

  def rowLimitExceeded(max: BigInt): HttpResponse = {
    BadRequest ~> errContent(RequestErrors.rowLimitExceeded, s"Row limit of $max exceeded",
                             Map(qpLimit -> JNumber(max)))
  }

  def noContentTypeResponse: HttpResponse = {
    internalServerError
  }

  def unparsableContentTypeResponse: HttpResponse = {
    internalServerError
  }

  def notJsonResponseResponse: HttpResponse = {
    internalServerError
  }

  def upstreamTimeoutResponse: HttpResponse = {
    GatewayTimeout
  }

  def resetResponse(response: HttpServletResponse): Unit = {
    if (response.isCommitted) ??? else response.reset()
  }


}
