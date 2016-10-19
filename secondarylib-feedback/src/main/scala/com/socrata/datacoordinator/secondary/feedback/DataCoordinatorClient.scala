package com.socrata.datacoordinator.secondary.feedback

import java.io.IOException

import com.rojoma.json.v3.ast.{JString, JObject, JValue, JArray}
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode}
import com.rojoma.json.v3.io.{JValueEventIterator, JsonReaderException}
import com.rojoma.json.v3.util.{JsonUtil, NoTag, SimpleHierarchyCodecBuilder, AutomaticJsonCodecBuilder}
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.secondary
import com.socrata.datacoordinator.util.collection.MutableColumnIdMap
import com.socrata.http.client.{HttpClient, RequestBuilder, SimpleHttpRequest}
import com.socrata.http.client.exceptions.{HttpClientException, ContentTypeException}

case class FeedbackFailure(reason: String, cause: Throwable) extends Exception(reason, cause)

object FeedbackFailure {

  def apply(reason: String): FeedbackFailure = FeedbackFailure(reason, null)
}

case class RowData[CV](pk: UserColumnId, rows: Iterator[secondary.Row[CV]])

object DataCoordinatorClient {
  def apply[CT,CV](httpClient: HttpClient,
                   hostAndPort: String => Option[(String, Int)],
                   retries: Int,
                   typeFromJValue: JValue => Option[CT]): (String, CT => JValue => Option[CV]) => DataCoordinatorClient[CT,CV] =
      new DataCoordinatorClient[CT,CV](httpClient, hostAndPort, retries, typeFromJValue, _, _)
}

class DataCoordinatorClient[CT,CV](httpClient: HttpClient,
                                   hostAndPort: String => Option[(String, Int)],
                                   retries: Int,
                                   typeFromJValue: JValue => Option[CT],
                                   datasetInternalName: String,
                                   fromJValueFunc: CT => JValue => Option[CV]) {

  val log = org.slf4j.LoggerFactory.getLogger(classOf[DataCoordinatorClient[CT,CV]])

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

  private def fail(message: String): Nothing = {
    log.error(message)
    throw FeedbackFailure(message)
  }

  private def fail(message: String, cause: Throwable): Nothing = {
    log.error(message)
    throw FeedbackFailure(message, cause)
  }

  private def failForResponse(message: String, code: Int, info: Any) =
    fail(s"$message from data-coordinator for status $code: $info")

  private val unexpected = "Received an unexpected error"
  private val uninterpretable = "Unable to interpret error response"

  private def unexpectedResponse(code: Int, response: ErrorResponse) =
    failForResponse(unexpected, code, response)

  private def uninterpretableResponse(code: Int, response: ErrorResponse) =
    failForResponse(uninterpretable, code, response)

  private def uninterpretableResponse(code: Int, error: DecodeError) =
    failForResponse(uninterpretable, code, error)

  private def retrying[T](actions: => T, remainingAttempts: Int = retries): T = {
    val failure = try {
      return actions
    } catch {
      case e: IOException => e
      case e: HttpClientException => e
      case e: JsonReaderException => e
    }

    log.info("Failure occurred while posting mutation script: {}", failure.getMessage)
    if (remainingAttempts > 0) retrying(actions, remainingAttempts - 1)
    else fail(s"Ran out of retry attempts after failure: ${failure.getMessage}", failure)
  }

  val UpdateDatasetDoesNotExist = "update.dataset.does-not-exist"
  val UpdateRowUnknownColumn = "update.row.unknown-column"
  val UpdateRowPrimaryKeyNonexistentOrNull = "update.row.primary-key-nonexistent-or-null"
  val ReqExportUnknownColumns = "req.export.unknown-columns"

  case class ErrorResponse(errorCode: String, data: JObject)
  implicit val erCodec = AutomaticJsonCodecBuilder[ErrorResponse]

  private def doRequest[T](request: SimpleHttpRequest,
                           message: String,
                           successHandle: JArray => T,
                           badRequestHandle: ErrorResponse => T,
                           serverErrorMessageExtra: String): Either[RequestFailure, T] = {
    retrying[Either[RequestFailure, T]] {
      httpClient.execute(request).run { resp =>
        val start = System.nanoTime()
        def logContentTypeFailure(v: => JValue): JValue =
          try {
            v
          } catch {
            case e: ContentTypeException =>
              log.warn("The response from data-coordinator was not a valid JSON content type! The request we sent was: {}", request.toString)
              fail("Unable to understand data-coordinator's response", e) // no retry here
          }

        resp.resultCode match {
          case 200 =>
            // success! ... well maybe...
            val end = System.nanoTime()
            log.info("{} in {}ms", message, (end - start) / 1000000)
            JsonDecode.fromJValue[JArray](logContentTypeFailure(resp.jValue())) match {
              case Right(response) => Right(successHandle(response))
              case Left(e) => fail(s"Response from data-coordinator was not an array: ${e.english}")
            }
          case 400 =>
            JsonDecode.fromJValue[ErrorResponse](logContentTypeFailure(resp.jValue())) match {
              case Right(response) => Right(badRequestHandle(response))
              case Left(e) => uninterpretableResponse(400, e)
            }
          case 404 =>
            // { "errorCode" : "update.dataset.does-not-exist"
            // , "data" : { "dataset" : "XXX.XXX, "data" : { "commandIndex" : 0 }
            // }
            JsonDecode.fromJValue[ErrorResponse](logContentTypeFailure(resp.jValue())) match {
              case Right(ErrorResponse(UpdateDatasetDoesNotExist, _)) => Left(DatasetDoesNotExist)
              case Right(response) => unexpectedResponse(404, response)
              case Left(e) => uninterpretableResponse(404, e)
            }
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
  }

  case class Column(c: UserColumnId, t: JValue)
  case class Schema(pk: UserColumnId, schema: Seq[Column])

  implicit val coCodec = AutomaticJsonCodecBuilder[Column]
  implicit val scCodec = AutomaticJsonCodecBuilder[Schema]

  /**
   * Export the rows of a dataset for the given columns
   * @param columnSet must contain at least on column
   */
  def exportRows(columnSet: Seq[UserColumnId], cookie: CookieSchema): Either[RequestFailure, Either[ColumnsDoNotExist, RowData[CV]]] = {
    require(columnSet.nonEmpty, "`columnSet` must be non-empty")
    log.info("Exporting rows for columns: {}", columnSet)
    val endpoint = datasetEndpoint.getOrElse(return Left(FailedToDiscoverDataCoordinator))
    val columns = columnSet.tail.foldLeft(columnSet.head.underlying) { (str, id) => str + "," + id.underlying }
    val builder = RequestBuilder(new java.net.URI(endpoint)).addParameter(("c", columns))

    def successHandle(response: JArray): Either[ColumnsDoNotExist, RowData[CV]] = {
      if (response.isEmpty) fail("Response from data-coordinator was an empty array.")

      JsonDecode.fromJValue[Schema](response.head) match {
        case Right(Schema(pk, schema)) =>
          val columns = schema.map { col =>
            (cookie.columnIdMap(col.c), typeFromJValue(col.t).getOrElse {
              fail(s"Could not derive column type for column ${col.c} from value: ${col.t}")
            })
          }.toArray
          val rows = response.tail.iterator.map {
            JsonDecode.fromJValue[JArray](_) match {
              case Right(row) =>
                assert(row.size == columns.size)
                val colIdMap = new MutableColumnIdMap[CV]
                row.iterator.zipWithIndex.foreach { case (jVal, index) =>
                  val (colId, typ) = columns(index)
                  colIdMap += ((colId, fromJValueFunc(typ)(jVal).getOrElse {
                    fail(s"Could not interpret column value of type $typ from value: $jVal")
                  }))
                }
                colIdMap.freeze()
              case Left(e) => failForResponse("Row data was not an array", 200, e.english)
            }
          }
          Right(RowData(pk, rows))
        case Left(e) => failForResponse("Response did not start with expected column schema", 200, e.english)
      }
    }

    def badRequestHandle(response: ErrorResponse): Either[ColumnsDoNotExist, RowData[CV]] = response match {
      case ErrorResponse(ReqExportUnknownColumns, data) =>
        JsonDecode.fromJValue[Set[UserColumnId]](data.getOrElse("columns", uninterpretableResponse(400, response))) match {
        case Right(unknown) if unknown.nonEmpty => Left(ColumnsDoNotExist(unknown))
        case _ => uninterpretableResponse(400, response)
      }
      case _ => unexpectedResponse(400, response)
    }

    doRequest(
      request = builder.get,
      message = s"Got row data for columns $columns",
      successHandle,
      badRequestHandle,
      serverErrorMessageExtra = s"my parameters where: c=$columns"
    )
  }

  sealed abstract class Response
  case class Upsert(typ: String, id: String, ver: String) extends Response
  case class NonfatalError(typ: String, err: String, id: Option[String]) extends Response

  implicit val upCodec = AutomaticJsonCodecBuilder[Upsert]
  implicit val neCodec = AutomaticJsonCodecBuilder[NonfatalError]
  implicit val reCodec = SimpleHierarchyCodecBuilder[Response](NoTag).branch[Upsert].branch[NonfatalError].build

  // this may throw a FeedbackFailure exception
  def postMutationScript(script: JArray, cookie: CookieSchema): Option[Either[RequestFailure, UpdateSchemaFailure]] = {
    val endpoint = datasetEndpoint.getOrElse(return Some(Left(FailedToDiscoverDataCoordinator)))
    val builder = RequestBuilder(new java.net.URI(endpoint))

    def body = JValueEventIterator(script)

    def successHandle(response: JArray): Option[UpdateSchemaFailure] = {
      assert(response.elems.length == 1, "Response contains more than one element")
      JsonDecode.fromJValue[JArray](response.elems.head) match {
        case Right(results) =>
          assert(results.elems.length == script.elems.length - 2, "Did not get one result for each upsert command that was sent")
          val rows = script.elems.slice(2, script.elems.length)
          results.elems.zip(rows).foreach { case (result, row) =>
            JsonDecode.fromJValue[Response](result) match {
              case Right(Upsert("update", _, _)) => // yay!
              case Right(NonfatalError("error", nonfatalError, Some(id))) if nonfatalError == "insert_in_update_only" ||  nonfatalError == "no_such_row_to_update" =>
                val JObject(fields) = JsonDecode.fromJValue[JObject](row).right.get // I just encoded this from a JObject
                val rowId = JsonDecode.fromJValue[JString](fields(cookie.primaryKey.underlying)).right.get.string
                if (rowId != id) return Some(PrimaryKeyColumnHasChanged) // else the row has been deleted
              case Right(other) => failForResponse("Unexpected response in array", 200, other)
              case Left(e) => failForResponse("Unable to interpret result in array", 200, e.english)
            }
          }
          None
        case Left(e) => failForResponse("Row data was not an array", 200, e.english)
      }
    }

    def badRequestHandle(response: ErrorResponse): Option[UpdateSchemaFailure] = {
      response.errorCode match {
        // { "errorCode" : "update.row.unknown-column"
        // , "data" : { "commandIndex" : 1, "commandSubIndex" : XXX, "dataset" : "XXX.XXX", "column" : "XXXX-XXXX"
        // }
        case UpdateRowUnknownColumn => response.data.get("column") match {
          case Some(JString(id)) =>
            val userId = new UserColumnId(id)
            if (id == cookie.primaryKey.underlying) Some(PrimaryKeyColumnDoesNotExist(userId))
            else Some(TargetColumnDoesNotExist(userId))
          case _ => uninterpretableResponse(400, response)
        }
        // { "errorCode" : "update.row.primary-key-nonexistent-or-null"
        // , "data" : { "commandIndex" : 1, "dataset" : "XXX.XXX"
        // }
        case UpdateRowPrimaryKeyNonexistentOrNull => Some(PrimaryKeyColumnHasChanged)
        case _ => unexpectedResponse(400, response)
      }
    }

    doRequest(
      request = builder.json(body),
      message = s"Posted mutation script with ${script.length - 2} row updates",
      successHandle,
      badRequestHandle,
      serverErrorMessageExtra = s"my mutation script was: ${JsonUtil.renderJson(script, pretty = false)}"
    ) match {
      case Left(failure) => Some(Left(failure))
      case Right(Some(failure)) => Some(Right(failure))
      case Right(None) => None
    }
  }
}
