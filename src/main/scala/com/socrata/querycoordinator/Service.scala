package com.socrata.querycoordinator

import javax.servlet.http.{HttpServletResponse, HttpServletRequest}

import dispatch._, Defaults._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import com.socrata.http.server.HttpResponse
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.ning.http.client._
import java.io.OutputStream
import com.socrata.thirdparty.asynchttpclient.{FAsyncHandler, BodyHandler}
import com.socrata.soql.environment.DatasetContext
import com.socrata.soql.types.SoQLAnalysisType
import com.socrata.soql.{SoQLAnalysis, SoQLAnalyzer}
import com.socrata.soql.exceptions.{TypecheckException, NoSuchColumn, DuplicateAlias, SoQLException}
import com.socrata.soql.collection.OrderedMap
import com.netflix.curator.x.discovery.ServiceInstance
import scala.util.control.ControlThrowable
import com.rojoma.json.io.{CompactJsonWriter, JsonReaderException, JsonReader}
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JValue, JString, JObject}
import scala.annotation.unchecked.uncheckedVariance
import javax.activation.{MimeTypeParseException, MimeType}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.annotation.tailrec
import java.nio.charset.{UnsupportedCharsetException, Charset}

sealed abstract class TimedFutureResult[+T]
case object FutureTimedOut extends TimedFutureResult[Nothing]
sealed abstract class FutureResult[+T] extends TimedFutureResult[T]
case class FutureCompleted[T](result: T) extends FutureResult[T]
case class FutureFailed(exception: Throwable) extends FutureResult[Nothing]

class EnrichedFuture[+A](val underlyingFuture: java.util.concurrent.Future[A @uncheckedVariance]) extends AnyVal {
  import java.util.concurrent._
  def apply(): FutureResult[A] =
    try {
      FutureCompleted(underlyingFuture.get())
    } catch {
      case e: ExecutionException =>
        FutureFailed(e.getCause)
    }

  def apply(duration: FiniteDuration): TimedFutureResult[A] =
    try {
      FutureCompleted(underlyingFuture.get(duration.toMillis min 1L, TimeUnit.MILLISECONDS))
    } catch {
      case _: TimeoutException =>
        FutureTimedOut
      case e: ExecutionException =>
        FutureFailed(e.getCause)
    }
}

object EnrichedFuture {
  import scala.language.implicitConversions
  implicit def fut2df[T](f: java.util.concurrent.Future[T]) = new EnrichedFuture(f)
}

import EnrichedFuture._

class Service(http: Http,
              secondaryProvider: ServiceProviderProvider[Void],
              getSchemaTimeout: FiniteDuration,
              responseResponseTimeout: FiniteDuration,
              responseDataTimeout: FiniteDuration,
              analyzer: SoQLAnalyzer[SoQLAnalysisType],
              analysisSerializer: (OutputStream, SoQLAnalysis[SoQLAnalysisType]) => Unit,
              schemaCache: (String, Schema) => Unit,
              schemaDecache: String => Option[Schema])
  extends (HttpServletRequest => HttpResponse)
{
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Service])

  def soqlErrorResponse(e: SoQLException): HttpResponse = {
    BadRequest
  }

  case class FinishRequest(response: HttpResponse) extends ControlThrowable
  def finishRequest(response: HttpResponse): Nothing = throw new FinishRequest(response)

  def errContent(msg: String, data: (String, JValue)*) = {
    val json = JObject(Map(
      "errorCode" -> JString(msg),
      "data" -> JObject(data.toMap)))
    val text = CompactJsonWriter.toString(json)
    Header("Content-type", "application/json; charset=utf-8") ~> Content(text)
  }

  def noSecondaryAvailable(dataset: String) = ServiceUnavailable ~> errContent(
    "query.datasource.unavailable",
    "dataset" -> JString(dataset))

  def internalServerError = InternalServerError ~> Content("Internal server error")
  def notFoundResponse(dataset: String) = NotFound ~> errContent(
    "query.dataset.does-not-exist",
    "dataset" -> JString(dataset))
  def noDatasetResponse = BadRequest ~> errContent("req.no-dataset-specified")
  def noQueryResponse = BadRequest ~> errContent("req.no-query-specified")
  def noContentTypeResponse = internalServerError
  def unparsableContentTypeResponse = internalServerError
  def notJsonResponseResponse = internalServerError
  def notModifiedResponse(newEtag: String) = NotModified ~> Header("etag", newEtag)
  def upstreamTimeoutResponse = internalServerError

  // returns the schema if the given service has this dataset, or None if it doesn't.
  def schemaFor(secondary: ServiceInstance[_], dataset: String): Option[Schema] = {
    val future = http.client.executeRequest((url(secondary.buildUriSpec) / "schema" <<? List("ds" -> dataset)).build)
    future(getSchemaTimeout) match {
      case FutureCompleted(response) if response.getStatusCode == HttpServletResponse.SC_OK =>
        val parsed = try {
          JsonReader.fromString(response.getResponseBody)
        } catch {
          case e: Exception =>
            log.error("Got an exception while parsing the returned schema", e)
            finishRequest(internalServerError)
        }
        val result = JsonCodec[Schema].decode(parsed)
        if(!result.isDefined) {
          log.error("Unable to convert the JSON to a schema")
          finishRequest(internalServerError)
        }
        result
      case FutureCompleted(response) if response.getStatusCode == HttpServletResponse.SC_NOT_FOUND =>
        None
      case FutureCompleted(response) =>
        log.error("Unexpected response code {} from request for schema of dataset {} from {}", response.getStatusCode.asInstanceOf[AnyRef], dataset, secondary.buildUriSpec)
        finishRequest(internalServerError)
      case FutureTimedOut =>
        future.cancel(true)
        log.error("get schema for dataset {} to to {} timed out", dataset:Any, secondary.buildUriSpec:Any)
        finishRequest(internalServerError)
      case FutureFailed(e) =>
        log.error("Got an exception while requesting schema for dataset {} from {}", dataset:Any, secondary.buildUriSpec:Any)
        finishRequest(internalServerError)
    }
  }

  def storeInCache(schema: Option[Schema], dataset: String): Option[Schema] = schema match {
    case s@Some(trueSchema) =>
      schemaCache(dataset, trueSchema)
      s
    case None =>
      None
  }

  def serializeAnalysis(analysis: SoQLAnalysis[SoQLAnalysisType]): String = {
    val baos = new java.io.ByteArrayOutputStream
    analysisSerializer(baos, analysis)
    new String(baos.toByteArray, "latin1")
  }

  def sendQuery[T](secondary: ServiceInstance[_], dataset: String, analysis: SoQLAnalysis[SoQLAnalysisType], schemaHash: String, ifNoneMatch: Option[String], handler: AsyncHandler[T]): java.util.concurrent.Future[T] = {
    val serializedAnalysis: String = serializeAnalysis(analysis)

    val req = ifNoneMatch.foldLeft(url(secondary.buildUriSpec) / "query" << Map("ds" -> dataset, "q" -> serializedAnalysis, "s" -> schemaHash)) { (r, etag) =>
      r <:< Map("If-None-Match" -> etag)
    }
    http.client.executeRequest(req.build, handler)
  }

  sealed abstract class RowDataResult
  case object FinishedSuccessfully extends RowDataResult
  case class SchemaOutOfDate(schema: Schema) extends RowDataResult
  case class OtherResult(response: HttpResponse) extends RowDataResult

  def checkSchemaOutOfDate(bytes: Array[Byte], headers: FluentCaseInsensitiveStringsMap): Option[Schema] = {
    Option(headers.getFirstValue("content-type")) map { contentTypeString =>
      val contentType =
        try { new MimeType(contentTypeString) }
        catch { case e: MimeTypeParseException =>
          log.error("Unable to parse content-type from secondary", e)
          return None
        }
      if(contentType.getBaseType != "application/json") {
        log.error("Response from secondary is not application/json")
        return None
      }
      val charsetString = Option(contentType.getParameter("charset")).getOrElse("latin1")
      val cs =
        try { Charset.forName(charsetString) }
        catch { case _: UnsupportedCharsetException =>
          log.error("Response is not in a known charset: " + charsetString)
          return None
        }
      val body = new String(bytes, cs)
      val json =
        try { JsonReader.fromString(body) }
        catch { case e: JsonReaderException =>
          log.error("Response is not valid JSON", e)
          return None
        }
      val obj: JObject = json.cast[JObject].getOrElse {
        log.error("Response is not a JSON object")
        return None
      }
      val errorCode = obj.get("errorCode").getOrElse {
        log.error("Response does not contain an errorCode field")
        return None
      }
      if(errorCode != JString("internal.schema-mismatch")) {
        return None // no need to log anything, it's some other kind of Conflict
      }

      val data = obj.get("data").getOrElse {
        log.error("Response does not contain a data field")
        return None
      }

      JsonCodec[Schema].decode(data).getOrElse {
        log.error("data object is not a valid Schema")
        return None
      }
    }
  }

  def processResponse(response: HttpServletResponse, onData: ()=>Unit, mutex: AnyRef, cancelled: AtomicBoolean)(status: HttpResponseStatus, headers: FluentCaseInsensitiveStringsMap): Either[RowDataResult, BodyHandler[RowDataResult]] = {
    mutex.synchronized {
      if(cancelled.get()) ???
      onData()
      status.getStatusCode match {
        case conflict@HttpServletResponse.SC_CONFLICT =>
          Right(new BodyHandler[RowDataResult] {
            val data = new java.io.ByteArrayOutputStream

            def done() = {
              onData()
              val bytes = data.toByteArray
              checkSchemaOutOfDate(bytes, headers) match {
                case Some(schema) =>
                  SchemaOutOfDate(schema)
                case None =>
                  response.setStatus(conflict)
                  headers.iterator.asScala.foreach { case entry =>
                    entry.getValue.asScala.foreach(response.addHeader(entry.getKey, _))
                  }
                  response.getOutputStream.write(bytes)
                  FinishedSuccessfully
              }
            }

            def bodyPart(bodyPart: HttpResponseBodyPart): Either[RowDataResult, BodyHandler[RowDataResult]] = mutex.synchronized {
              if(!cancelled.get) {
                onData()
                bodyPart.writeTo(data)
                if(data.size > 10*1024*1024) {
                  log.error("Received too much data on a Conflict response")
                  return Left(OtherResult(internalServerError))
                }
              }
              Right(this)
            }
          })
        case other =>
          response.setStatus(other)
          headers.iterator.asScala.foreach { case ent =>
            ent.getValue.asScala.foreach(response.addHeader(ent.getKey, _))
          }
          val output = response.getOutputStream
          Right(new BodyHandler[RowDataResult] {
            def done() = FinishedSuccessfully

            def bodyPart(bodyPart: HttpResponseBodyPart) = mutex.synchronized {
              if(!cancelled.get) {
                onData()
                bodyPart.writeTo(output)
              }
              Right(this)
            }
          })
      }
    }
  }

  def secondary(dataset: String) = Option(secondaryProvider.provider(0).getInstance).getOrElse {
    finishRequest(noSecondaryAvailable(dataset))
  }

  def getAndCacheSchema(dataset: String) =
    schemaFor(secondary(dataset), dataset) match {
      case Some(newSchema) =>
        schemaCache(dataset, newSchema)
        newSchema
      case None =>
        finishRequest(notFoundResponse(dataset))
    }

  def apply(req: HttpServletRequest) = { resp =>
    val originalThreadName = Thread.currentThread.getName
    try {
      Thread.currentThread.setName(Thread.currentThread.getId + " / " + req.getMethod + " " + req.getRequestURI)

      val dataset = Option(req.getParameter("ds")).getOrElse {
        finishRequest(noDatasetResponse)
      }
      val query = Option(req.getParameter("q")).getOrElse {
        finishRequest(noQueryResponse)
      }
      val ifNoneMatch = Option(req.getHeader("if-none-match"))

      // A little spaghetti never hurt anybody!
      // Ok, so the general flow of this code is:
      //   1. Look up the dataset's schema (in cache or, if
      //     necessary, from the secondary)
      //   2. Analyze the query -- if this fails, and the failure
      //     was a type- or name-related error, and the schema came
      //     from the cache, refresh the schema and try again.
      //   3. Make the actual "give me data" request.
      // That last step is the most complex, because it is a
      // potentially long-running thing.  We need to make an
      // HTTP request and then wait for either the future to
      // complete or a timeout to occur.  The timeout is broken
      // into two stages:
      //   1. Response headers received
      //   2. Response data received
      // Finally, a future completion can take one of several
      // forms.  It can be a success (the request was
      // made and the data was transferred to the client in the
      // background), a retry (the schema used was out-of-date by
      // the time the query was run; this will give us a new schema
      // to go back to the first step with) or "other" (the request
      // handler has given us an HttpResponse object to use to populate
      // the servlet response).
      @tailrec
      def loop(rawSchema: Schema, isFresh: Boolean) {
        implicit val datasetCtx = new DatasetContext[SoQLAnalysisType] {
          val schema = OrderedMap(rawSchema.schema.toSeq.sortBy(_._1) : _*)
        }

        (try {
          Some(analyzer.analyzeFullQuery(query))
        } catch {
          case (_: DuplicateAlias | _: NoSuchColumn | _: TypecheckException) if !isFresh =>
            None
          case e: SoQLException =>
            finishRequest(soqlErrorResponse(e))
        }) match {
          case Some(analysis) =>
            val lastDataReceived = new AtomicLong(System.nanoTime())
            val cancelled = new AtomicBoolean(false)
            def onData() {
              lastDataReceived.set(System.nanoTime())
            }
            val mutex = new Object
            val handler = FAsyncHandler()(processResponse(resp, onData, mutex, cancelled))
            val future = sendQuery(secondary(dataset), dataset, analysis, rawSchema.hash, ifNoneMatch, handler)
            def maybeAbandon(duration: FiniteDuration): Boolean =
              mutex.synchronized {
                val now = System.nanoTime()
                if((now - lastDataReceived.get) / 1000000 > duration.toMillis) {
                  future.cancel(true)
                  cancelled.set(true)
                  true
                } else {
                  false
                }
              }
            val result = future(responseResponseTimeout) match {
              case FutureTimedOut =>
                if(maybeAbandon(responseResponseTimeout)) {
                  log.error("Never even got headers from upstream.  Abandoning request.")
                  finishRequest(upstreamTimeoutResponse)
                }

                @tailrec
                def loopForTimeout(): RowDataResult = {
                  future(responseDataTimeout) match {
                    case FutureTimedOut =>
                      if(maybeAbandon(responseDataTimeout)) {
                        log.error("Upstream took too long.  Abandoning request")
                        finishRequest(upstreamTimeoutResponse)
                      } else {
                        loopForTimeout()
                      }
                    case FutureCompleted(futureResult) =>
                      futureResult
                    case FutureFailed(err) =>
                      log.error("Unexpected exception while processing data response from secondary", err)
                      finishRequest(internalServerError)
                  }
                }
                loopForTimeout()
              case FutureCompleted(futureResult) =>
                futureResult
              case FutureFailed(err) =>
                log.error("Unexpected exception while processing header response from secondary", err)
                finishRequest(internalServerError)
            }
            result match {
              case FinishedSuccessfully =>
                // ok
              case SchemaOutOfDate(schema) =>
                schemaCache(dataset, schema)
                loop(schema, true)
              case OtherResult(response) =>
                finishRequest(response)
            }
          case None =>
            loop(getAndCacheSchema(dataset), true)
        }
      }
      schemaDecache(dataset) match {
        case Some(schema) =>
          loop(schema, false)
        case None =>
          loop(getAndCacheSchema(dataset), true)
      }
    } catch {
      case FinishRequest(response) =>
        if(resp.isCommitted) ???
        else { resp.reset(); response(resp) }
    } finally {
      Thread.currentThread.setName(originalThreadName)
    }
  }
}
