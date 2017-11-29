package com.socrata.datacoordinator.secondary.feedback

import java.io.IOException

import com.rojoma.json.util.JsonArrayIterator.ElementDecodeException
import com.rojoma.json.v3.ast.{JArray, JObject, JString, JValue}
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode}
import com.rojoma.json.v3.io.{JValueEventIterator, JsonLexException, JsonReaderException}
import com.rojoma.json.v3.util._
import com.rojoma.simplearm.v2.{ResourceScope, using}
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.secondary
import com.socrata.datacoordinator.util.collection.MutableColumnIdMap
import com.socrata.http.client.{HttpClient, RequestBuilder, SimpleHttpRequest}
import com.socrata.http.client.exceptions.{ContentTypeException, HttpClientException}

case class RowData[CV](pk: UserColumnId, rows: Iterator[secondary.Row[CV]])

abstract class DataCoordinatorClient[CT, CV](typeFromJValue: JValue => Option[CT],
                                             datasetInternalName: String,
                                             fromJValueFunc: CT => JValue => Option[CV]) {

  /**
   * Export the rows of a dataset for the given columns
   * @param columnSet must contain at least on column
   */
  def exportRows(columnSet: Seq[UserColumnId],
                 cookie: CookieSchema,
                 resourceScope: ResourceScope): Either[RequestFailure, Either[ColumnsDoNotExist, RowData[CV]]]

  /**
   * Post mutation script to data-coordinator
   * @return On success return None
   *         On failure return Some RequestFailure or UpdateSchemaFailure
   */
  def postMutationScript(script: JArray, cookie: CookieSchema): Option[Either[RequestFailure, UpdateSchemaFailure]]

}

object HttpDataCoordinatorClient {
  def apply[CT,CV](httpClient: HttpClient,
                   hostAndPort: String => Option[(String, Int)],
                   retries: Int,
                   typeFromJValue: JValue => Option[CT]): (String, CT => JValue => Option[CV]) => HttpDataCoordinatorClient[CT,CV] =
      new HttpDataCoordinatorClient[CT,CV](httpClient, hostAndPort, retries, typeFromJValue, _, _)
}

class HttpDataCoordinatorClient[CT,CV](httpClient: HttpClient,
                                       hostAndPort: String => Option[(String, Int)],
                                       retries: Int,
                                       typeFromJValue: JValue => Option[CT],
                                       datasetInternalName: String,
                                       fromJValueFunc: CT => JValue => Option[CV])
  extends DataCoordinatorClient[CT,CV](typeFromJValue, datasetInternalName, fromJValueFunc) {

  val log = org.slf4j.LoggerFactory.getLogger(classOf[HttpDataCoordinatorClient[CT,CV]])

  private def datasetEndpoint: Option[String] = {
    datasetInternalName.lastIndexOf('.') match {
      case -1 =>
        log.error("Could not extract data-coordinator instance name from dataset: {}", datasetInternalName)
        throw new Exception(s"Could not extract data-coordinator instance name from dataset: $datasetInternalName")
      case n =>
        hostAndPort(datasetInternalName.substring(0, n)) match {
          case Some((host, port)) => Some(s"http://$host:$port/dataset/$datasetInternalName")
          case None => None
        }
    }
  }

  private def unexpectedError[T](message: String, cause: Throwable = null): Left[UnexpectedError, T] = {
    log.error(message)
    Left(UnexpectedError(message, cause))
  }

  private def unexpectedErrorForResponse(message: String, code: Int, info: Any, cause: Exception = null) =
    unexpectedError(s"$message from data-coordinator for status $code: $info", cause)

  private val unexpected = "Received an unexpected error"
  private val uninterpretable = "Unable to interpret error response"

  private def unexpectedResponse(code: Int, response: ErrorResponse) =
    unexpectedErrorForResponse(unexpected, code, response)

  private def uninterpretableResponse(code: Int, response: ErrorResponse) =
    unexpectedErrorForResponse(uninterpretable, code, response)

  private def uninterpretableResponse(code: Int, error: DecodeError) =
    unexpectedErrorForResponse(uninterpretable, code, error)

  private def retrying[T](actions: => Either[RequestFailure, T], remainingAttempts: Int = retries): Either[RequestFailure, T] = {
    val failure = try {
      return actions
    } catch {
      case e: IOException => e
      case e: HttpClientException => e
      case e: JsonReaderException => e
    }

    log.info("Failure occurred while posting mutation script: {}", failure.getMessage)
    if (remainingAttempts > 0) retrying[T](actions, remainingAttempts - 1)
    else unexpectedError(s"Ran out of retry attempts after failure: ${failure.getMessage}", failure)
  }

  val UpdateDatasetDoesNotExist = "update.dataset.does-not-exist"
  val UpdateRowUnknownColumn = "update.row.unknown-column"
  val ReqExportUnknownColumns = "req.export.unknown-columns"

  case class ErrorResponse(errorCode: String, data: JObject)
  implicit val erCodec = AutomaticJsonCodecBuilder[ErrorResponse]

  private def doRequest[T](request: SimpleHttpRequest,
                           message: String,
                           successHandle: Iterator[JValue]=> Either[UnexpectedError, T],
                           badRequestHandle: ErrorResponse => Either[UnexpectedError, T],
                           serverErrorMessageExtra: String,
                           resourceScope: ResourceScope): Either[RequestFailure, T] = {
    retrying[T] {
      val resp = httpClient.execute(request, resourceScope)
      val start = System.nanoTime()
      def logContentTypeFailure[V](v: => JValue)(f: JValue => Either[RequestFailure, T]): Either[RequestFailure, T] =
        try {
          f(v)
        } catch {
          case e: ContentTypeException =>
            log.warn("The response from data-coordinator was not a valid JSON content type! The request we sent was: {}", request.toString)
              unexpectedError("Unable to understand data-coordinator's response", e) // no retry here
        }

      resp.resultCode match {
        case 200 =>
          // success! ... well maybe...
          val end = System.nanoTime()
          log.info("{} in {}ms", message, (end - start) / 1000000) // this is kind of a lie... hmm
          val response = JsonArrayIterator.fromReader[JValue](resp.reader())
          successHandle(resourceScope.openUnmanaged(response, transitiveClose = List(resp)))
        case 400 =>
          logContentTypeFailure(resp.jValue()) { JsonDecode.fromJValue[ErrorResponse](_) match {
            case Right(response) => badRequestHandle(response)
            case Left(e) => uninterpretableResponse(400, e)
          }}
        case 404 =>
          // { "errorCode" : "update.dataset.does-not-exist"
          // , "data" : { "dataset" : "XXX.XXX, "data" : { "commandIndex" : 0 }
          // }
          logContentTypeFailure(resp.jValue()) { JsonDecode.fromJValue[ErrorResponse](_) match {
            case Right(ErrorResponse(UpdateDatasetDoesNotExist, _)) => Left(DatasetDoesNotExist)
            case Right(response) => unexpectedResponse(404, response)
            case Left(e) => uninterpretableResponse(404, e)
          }}
        case 409 =>
          // come back later!
          Left(DataCoordinatorBusy) // no retry
        case other =>
          if (other == 500) {
            log.warn("Scream! 500 from data-coordinator! Going to retry; {}", serverErrorMessageExtra)
          }
          // force a retry
          throw new IOException(s"Unexpected result code $other from data-coordinator")
      }
    }
  }

  case class Column(c: UserColumnId, t: JValue)
  case class Schema(pk: UserColumnId, schema: Seq[Column])

  implicit val coCodec = AutomaticJsonCodecBuilder[Column]
  implicit val scCodec = AutomaticJsonCodecBuilder[Schema]

  /**
   * Export the rows of a dataset for the given columns
   * @param columnSet must contain at least on column
   */
  def exportRows(columnSet: Seq[UserColumnId],
                 cookie: CookieSchema,
                 resourceScope: ResourceScope): Either[RequestFailure, Either[ColumnsDoNotExist, RowData[CV]]] = {
    require(columnSet.nonEmpty, "`columnSet` must be non-empty")
    log.info("Exporting rows for columns: {}", columnSet)
    val endpoint = datasetEndpoint.getOrElse(return Left(FailedToDiscoverDataCoordinator))
    val columns = columnSet.tail.foldLeft(columnSet.head.underlying) { (str, id) => str + "," + id.underlying }
    val builder = RequestBuilder(new java.net.URI(endpoint)).addParameter(("c", columns))

    def successHandle(response: Iterator[JValue]): Either[UnexpectedError, Either[ColumnsDoNotExist, RowData[CV]]] = {
      if (response.isEmpty) return unexpectedError("Response from data-coordinator was an empty array.")

      JsonDecode.fromJValue[Schema](response.next()) match {
        case Right(Schema(pk, schema)) =>
          val columns = schema.map { col =>
            (cookie.columnIdMap(col.c), typeFromJValue(col.t).getOrElse {
              return unexpectedError(s"Could not derive column type for column ${col.c} from value: ${col.t}")
            })
          }.toArray
          val rows = response.map {
            JsonDecode.fromJValue[JArray](_) match {
              case Right(row) =>
                assert(row.size == columns.length)
                val colIdMap = new MutableColumnIdMap[CV]
                row.iterator.zipWithIndex.foreach { case (jVal, index) =>
                  val (colId, typ) = columns(index)
                  colIdMap += ((colId, fromJValueFunc(typ)(jVal).getOrElse {
                    return unexpectedError(s"Could not interpret column value of type $typ from value: $jVal")
                  }))
                }
                colIdMap.freeze()
              case Left(e) => return unexpectedErrorForResponse("Row data was not an array", 200, e.english)
            }
          }
          Right(Right(resourceScope.openUnmanaged(RowData(pk, rows), transitiveClose = List(response))))
        case Left(e) => unexpectedErrorForResponse("Response did not start with expected column schema", 200, e.english)
      }
    }

    def badRequestHandle(response: ErrorResponse): Either[UnexpectedError, Either[ColumnsDoNotExist, RowData[CV]]] = response match {
      case ErrorResponse(ReqExportUnknownColumns, data) =>
        JsonDecode.fromJValue[Set[UserColumnId]](data.getOrElse("columns", return uninterpretableResponse(400, response))) match {
        case Right(unknown) if unknown.nonEmpty => Right(Left(ColumnsDoNotExist(unknown)))
        case _ => uninterpretableResponse(400, response)
      }
      case _ => unexpectedResponse(400, response)
    }

    doRequest(
      request = builder.get,
      message = s"Got row data for columns $columns",
      successHandle,
      badRequestHandle,
      serverErrorMessageExtra = s"my parameters where: c=$columns",
      resourceScope
    )
  }

  sealed abstract class Response
  case class Upsert(typ: String, id: JValue, ver: String) extends Response
  case class NonfatalError(typ: String, err: String, id: Option[JValue]) extends Response

  implicit val upCodec = AutomaticJsonCodecBuilder[Upsert]
  implicit val neCodec = AutomaticJsonCodecBuilder[NonfatalError]
  implicit val reCodec = SimpleHierarchyCodecBuilder[Response](NoTag).branch[Upsert].branch[NonfatalError].build

  override def postMutationScript(script: JArray, cookie: CookieSchema): Option[Either[RequestFailure, UpdateSchemaFailure]] = {
    val endpoint = datasetEndpoint.getOrElse(return Some(Left(FailedToDiscoverDataCoordinator)))
    val builder = RequestBuilder(new java.net.URI(endpoint))

    def body = JValueEventIterator(script)

    def successHandle(response: Iterator[JValue]): Either[UnexpectedError, Option[UpdateSchemaFailure]] = {
      try {
        assert(response.hasNext, "Response contains no elements")
        JsonDecode.fromJValue[JArray](response.next()) match {
          case Right(results) =>
            assert(!response.hasNext, "Response contains more than one element")
            assert(results.elems.length == script.elems.length - 2, "Did not get one result for each upsert command that was sent")
            val rows = script.elems.slice(2, script.elems.length)
            results.elems.zip(rows).foreach { case (result, row) =>
              JsonDecode.fromJValue[Response](result) match {
                case Right(Upsert("update", _, _)) => // yay!
                case Right(NonfatalError("error", nonfatalError, Some(_))) if nonfatalError == "insert_in_update_only" || nonfatalError == "no_such_row_to_update" =>
                // the row has been deleted, nothing more to do here
                case Right(other) => unexpectedErrorForResponse("Unexpected response in array", 200, other)
                case Left(e) => unexpectedErrorForResponse("Unable to interpret result in array", 200, e.english)
              }
            }
            Right(None)
          case Left(e) => unexpectedErrorForResponse("Row data was not an array", 200, e.english)
        }
      } catch {
        case e: JsonLexException => unexpectedErrorForResponse("Unable to parse response as JSON", 200, e.message, e)
        case e: ElementDecodeException => unexpectedErrorForResponse("Unable to parse an element of the response as JSON", 200, e.position, e)
      }
    }

    def badRequestHandle(response: ErrorResponse): Either[UnexpectedError, Option[UpdateSchemaFailure]] = {
      response.errorCode match {
        // { "errorCode" : "update.row.unknown-column"
        // , "data" : { "commandIndex" : 1, "commandSubIndex" : XXX, "dataset" : "XXX.XXX", "column" : "XXXX-XXXX"
        // }
        case UpdateRowUnknownColumn => response.data.get("column") match {
          case Some(JString(id)) if id != cookie.systemId.underlying =>
            Right(Some(TargetColumnDoesNotExist(new UserColumnId(id))))
          case _ => uninterpretableResponse(400, response)
        }
        case _ => unexpectedResponse(400, response)
      }
    }

    val requestResult = using(new ResourceScope("post mutation script")) { resourceScope =>
      doRequest(
        request = builder.json(body),
        message = s"Posted mutation script with ${script.length - 2} row updates",
        successHandle,
        badRequestHandle,
        serverErrorMessageExtra = s"my mutation script was: ${JsonUtil.renderJson(script, pretty = false)}",
        resourceScope
      ) match {
        case Left(failure) => Some(Left(failure))
        case Right(Some(failure)) => Some(Right(failure))
        case Right(None) => None
      }
    }

    requestResult
  }
}
