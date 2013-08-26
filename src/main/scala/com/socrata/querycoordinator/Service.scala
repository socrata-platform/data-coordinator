package com.socrata.querycoordinator

import javax.servlet.http.{HttpServletResponse, HttpServletRequest}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import com.socrata.http.server.HttpResponse
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import java.io._
import com.socrata.thirdparty.asynchttpclient.{FAsyncHandler, BodyHandler}
import com.socrata.soql.environment.{TypeName, ColumnName, DatasetContext}
import com.socrata.soql.types.{SoQLType, SoQLAnalysisType}
import com.socrata.soql.SoQLAnalyzer
import com.socrata.soql.exceptions._
import com.socrata.soql.collection.OrderedMap
import com.netflix.curator.x.discovery.ServiceInstance
import scala.util.control.ControlThrowable
import com.rojoma.json.io._
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JNumber, JValue, JObject}
import scala.annotation.unchecked.uncheckedVariance
import scala.annotation.tailrec
import com.rojoma.simplearm.Managed
import com.socrata.soql.exceptions.NoSuchColumn
import com.socrata.soql.exceptions.UnterminatedString
import com.socrata.soql.exceptions.UnexpectedCharacter
import com.socrata.soql.exceptions.DuplicateAlias
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.exceptions.BadParse
import com.socrata.soql.exceptions.TypeMismatch
import com.rojoma.json.ast.JString
import com.socrata.soql.exceptions.UnexpectedEscape
import com.socrata.soql.exceptions.AggregateInUngroupedContext
import com.socrata.soql.exceptions.CircularAliasDefinition
import com.socrata.soql.exceptions.UnexpectedEOF
import com.socrata.soql.exceptions.AmbiguousCall
import com.socrata.soql.exceptions.NoSuchFunction
import com.socrata.soql.exceptions.ColumnNotInGroupBys
import com.socrata.soql.exceptions.BadUnicodeEscapeCharacter
import com.socrata.soql.exceptions.UnicodeCharacterOutOfRange
import com.socrata.soql.exceptions.RepeatedException
import com.rojoma.simplearm.util._
import com.socrata.http.client.{Response, RequestBuilder, HttpClient}
import com.socrata.http.common.AuxiliaryData
import java.util.Locale
import com.google.common.collect.{HashBiMap, BiMap}
import com.rojoma.json.util.JsonUtil
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.routing.SimpleRouteContext._

class Service(http: HttpClient,
              secondaryProvider: ServiceProviderProvider[AuxiliaryData],
              getSchemaTimeout: FiniteDuration,
              responseResponseTimeout: FiniteDuration,
              responseDataTimeout: FiniteDuration,
              analyzer: SoQLAnalyzer[SoQLAnalysisType],
              analysisSerializer: (OutputStream, SoQLAnalysis[String, SoQLAnalysisType]) => Unit,
              schemaCache: (String, Schema) => Unit,
              schemaDecache: String => Option[Schema],
              secondaryInstance: String)
  extends (HttpServletRequest => HttpResponse)
{
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Service])

  // FIXME: don't use this internal rojoma-json API.
  def soqlErrorCode(e: SoQLException) =
    "query.soql." + com.rojoma.`json-impl`.util.CamelSplit(e.getClass.getSimpleName).map(_.toLowerCase).mkString("-")

  def soqlErrorData(e: SoQLException): Map[String, JValue] = e match {
    case AggregateInUngroupedContext(func, clause, _) =>
      Map(
        "function" -> JString(func.name),
        "clause" -> JString(clause))
    case ColumnNotInGroupBys(column, _) =>
      Map("column" -> JString(column.name))
    case RepeatedException(column, _) =>
      Map("column" -> JString(column.name))
    case DuplicateAlias(name, _) =>
      Map("name" -> JString(name.name))
    case NoSuchColumn(column, _) =>
      Map("column" -> JString(column.name))
    case CircularAliasDefinition(name, _) =>
      Map("name" -> JString(name.name))
    case UnexpectedEscape(c, _) =>
      Map("character" -> JString(c.toString))
    case BadUnicodeEscapeCharacter(c, _) =>
      Map("character" -> JString(c.toString))
    case UnicodeCharacterOutOfRange(x, _) =>
      Map("number" -> JNumber(x))
    case UnexpectedCharacter(c, _) =>
      Map("character" -> JString(c.toString))
    case UnexpectedEOF(_) =>
      Map.empty
    case UnterminatedString(_) =>
      Map.empty
    case BadParse(msg, _) =>
      // TODO: this needs to be machine-readable
      Map("message" -> JString(msg))
    case NoSuchFunction(name, arity, _) =>
      Map(
        "function" -> JString(name.name),
        "arity" -> JNumber(arity))
    case TypeMismatch(name, actual, _) =>
      Map(
        "function" -> JString(name.name),
        "type" -> JString(actual.name))
    case AmbiguousCall(name, _) =>
      Map("function" -> JString(name.name))
  }

  def soqlErrorResponse(dataset: String, e: SoQLException): HttpResponse = BadRequest ~> errContent(
    soqlErrorCode(e),
    "data" -> JObject(soqlErrorData(e) ++ Map(
      "dataset" -> JString(dataset),
      "position" -> JObject(Map(
        "row" -> JNumber(e.position.line),
        "column" -> JNumber(e.position.column),
        "line" -> JString(e.position.longString)
      ))
    ))
  )


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
  def unknownColumnIds(columnIds: Seq[String]) = BadRequest ~> errContent("req.unknown.column-ids", "columns" -> JsonCodec.toJValue(columnIds))
  def noContentTypeResponse = internalServerError
  def unparsableContentTypeResponse = internalServerError
  def notJsonResponseResponse = internalServerError
  def notModifiedResponse(newEtag: String) = NotModified ~> Header("etag", newEtag)
  def upstreamTimeoutResponse = internalServerError

  def reqBuilder(secondary: ServiceInstance[AuxiliaryData]) = {
    val pingTarget = for {
      auxData <- Option(secondary.getPayload)
      pingInfo <- auxData.livenessCheckInfo
    } yield pingInfo
    val b = RequestBuilder(secondary.getAddress).
      livenessCheckInfo(pingTarget)
    if(secondary.getSslPort != null) {
      b.secure(true).port(secondary.getSslPort)
    } else if(secondary.getPort != null) {
      b.port(secondary.getPort)
    } else {
      b
    }
  }

  // returns the schema if the given service has this dataset, or None if it doesn't.
  def schemaFor(secondary: ServiceInstance[AuxiliaryData], dataset: String): Option[Schema] = {
    try {
      val req = reqBuilder(secondary).
        p("schema").
        q("ds" -> dataset).
        get
      for(response <- http.execute(req)) yield {
        response.resultCode match {
          case HttpServletResponse.SC_OK =>
            val result = try {
              response.asValue[Schema]()
            } catch {
              case e: Exception =>
                log.error("Got an exception while parsing the returned schema", e)
                finishRequest(internalServerError)
            }
            if(!result.isDefined) {
              log.error("Unable to convert the JSON to a schema")
              finishRequest(internalServerError)
            }
            result
          case HttpServletResponse.SC_NOT_FOUND =>
            None
          case otherCode =>
            log.error("Unexpected response code {} from request for schema of dataset {} from {}:{}", otherCode.asInstanceOf[AnyRef], dataset.asInstanceOf[AnyRef], secondary.getAddress, secondary.getPort)
            finishRequest(internalServerError)
        }
      }
    } catch {
      case e: Exception =>
        log.error("Got an exception while requesting schema for dataset {} from {}:{}", dataset:AnyRef, secondary.getAddress:AnyRef, secondary.getPort, e)
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

  def serializeAnalysis(analysis: SoQLAnalysis[String, SoQLAnalysisType]): String = {
    val baos = new java.io.ByteArrayOutputStream
    analysisSerializer(baos, analysis)
    new String(baos.toByteArray, "latin1")
  }

 private def sendQuery[T](secondary: ServiceInstance[AuxiliaryData], dataset: String, analysis: SoQLAnalysis[String, SoQLAnalysisType], schemaHash: String, ifNoneMatch: Option[String], jsonizedColumnIdMap: String): Managed[Response] = {
    val serializedAnalysis: String = serializeAnalysis(analysis)

    val req = ifNoneMatch.foldLeft(reqBuilder(secondary).
      p("query")) { (r, etag) =>
      r.addHeader("If-None-Match" -> etag)
    }
    http.execute(req.form(Map(
      "dataset" -> dataset,
      "query" -> serializedAnalysis,
      "schemaHash" -> schemaHash,
      "columnIdMap" -> jsonizedColumnIdMap
    )))
  }

  sealed abstract class RowDataResult
  case object FinishedSuccessfully extends RowDataResult
  case class SchemaOutOfDate(schema: Schema) extends RowDataResult
  case class OtherResult(response: HttpResponse) extends RowDataResult

  def checkSchemaOutOfDate(json: JValue): Option[Schema] = {
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

    val schema = JsonCodec[Schema].decode(data).getOrElse {
      log.error("data object is not a valid Schema")
      return None
    }

    Some(schema)
  }

  def secondary(dataset: String) = Option(secondaryProvider.provider(secondaryInstance).getInstance).getOrElse {
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

  trait QCResource extends SimpleResource

  object VersionResource extends QCResource {
    val responseString = for {
      stream <- managed(getClass.getClassLoader.getResourceAsStream("query-coordinator-version.json"))
      source <- managed(scala.io.Source.fromInputStream(stream)(scala.io.Codec.UTF8))
    } yield source.mkString

    val response =
      OK ~> ContentType("application/json; charset=utf-8") ~> Content(responseString)

    override val get = (_: HttpServletRequest) => response
  }

  object QueryResource extends QCResource {
    override val get = process _
    override val post = process _
    override val put = process _
  }

  // Little dance because "/*" doesn't compile yet and I haven't
  // decided what its canonical target should be (probably "/query")
  val routingTable = Routes(
    Route("/{String}/*", (_: Any, _: Any) => QueryResource),
    Route("/{String}", (_: Any) => QueryResource),
    Route("/version", VersionResource)
  )

  def apply(req: HttpServletRequest) =
    routingTable(req.requestPath) match {
      case Some(resource) => resource(req)
      case None => NotFound
    }

  private def process(req: HttpServletRequest)(resp: HttpServletResponse) {
    val originalThreadName = Thread.currentThread.getName
    try {
      Thread.currentThread.setName(Thread.currentThread.getId + " / " + req.getMethod + " " + req.getRequestURI)

      val dataset = Option(req.getParameter("ds")).getOrElse {
        finishRequest(noDatasetResponse)
      }
      val query = Option(req.getParameter("q")).getOrElse {
        finishRequest(noQueryResponse)
      }
      val jsonizedColumnIdMap = Option(req.getParameter("idMap")).getOrElse {
        return (BadRequest ~> Content("no idMap provided"))(resp)
      }
      val columnIdMap: BiMap[ColumnName, String] = try {
        val converted = JsonUtil.parseJson[Map[String,String]](jsonizedColumnIdMap) match {
          case Some(scalaMap) =>
            scalaMap.map { case (col, typ) => ColumnName(col) -> typ }
          case None =>
            finishRequest(BadRequest ~> Content("idMap not an object whose values are strings"))
        }
        HashBiMap.create(converted.asJava)
      } catch {
        case e: JsonReaderException =>
          finishRequest(BadRequest ~> Content("idMap not parsable as JSON"))
      }
      val ifNoneMatch = req.header("if-none-match")

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
        (try {
          columnIdMap.values.iterator.asScala.filterNot(rawSchema.schema.contains(_)).toList match {
            case Nil => // no unknown columns
              implicit val datasetCtx = new DatasetContext[SoQLAnalysisType] {
                val schema = OrderedMap(columnIdMap.asScala.mapValues(rawSchema.schema).toSeq.sortBy(_._1) : _*)
              }
              Some(analyzer.analyzeFullQuery(query))
            case columnIds =>
              if(isFresh) finishRequest(unknownColumnIds(columnIds))
              else None
          }
        } catch {
          case (_: DuplicateAlias | _: NoSuchColumn | _: TypecheckException) if !isFresh =>
            None
          case e: SoQLException =>
            finishRequest(soqlErrorResponse(dataset, e))
        }) match {
          case Some(analysis) =>
            val outcome = for(result <- sendQuery(secondary(dataset), dataset, analysis.mapColumnIds(columnIdMap.asScala), rawSchema.hash, ifNoneMatch, jsonizedColumnIdMap)) yield {
              result.resultCode match {
                case HttpServletResponse.SC_CONFLICT =>
                  val json = result.asJValue()
                  checkSchemaOutOfDate(json) match {
                    case None =>
                      resp.setStatus(HttpServletResponse.SC_CONFLICT)
                      val headersToRemove = Set("content-length", "content-encoding")
                      result.headerNames.map(_.toLowerCase(Locale.US)).filterNot(headersToRemove).foreach { h =>
                        result.headers(h).foreach(resp.addHeader(h, _))
                      }
                      CompactJsonWriter.toWriter(resp.getWriter, json)
                      FinishedSuccessfully
                    case Some(s) =>
                      SchemaOutOfDate(s)
                  }
                case other =>
                  resp.setStatus(other)
                  val headersToRemove = Set("content-length", "content-encoding")
                  result.headerNames.map(_.toLowerCase(Locale.US)).filterNot(headersToRemove).foreach { h =>
                    result.headers(h).foreach(resp.addHeader(h, _))
                  }
                  using(new BufferedWriter(resp.getWriter)) { w =>
                    EventTokenIterator(result.asJsonEvents()).foreach { t =>
                      w.write(t.asFragment)
                    }
                  }
                  FinishedSuccessfully
              }
            }
            outcome match {
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
