package com.socrata.querycoordinator

import java.io._

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.control.ControlThrowable

import com.rojoma.json.ast.{JNumber, JObject, JString, JValue}
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.io._
import com.rojoma.json.util.JsonUtil
import com.rojoma.simplearm.util._
import com.socrata.http.client.RequestBuilder
import com.socrata.http.common.AuxiliaryData
import com.socrata.http.common.util.HttpUtils
import com.socrata.http.server.HttpResponse
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.routing.SimpleRouteContext._
import com.socrata.http.server.util.RequestId
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.exceptions._
import com.socrata.soql.types.SoQLAnalysisType
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.apache.curator.x.discovery.ServiceInstance
import org.apache.http.HttpStatus
import org.joda.time.{DateTime, Interval}
import org.joda.time.format.ISODateTimeFormat

/**
 * Main HTTP resource servicing class
 */
class Service(secondaryProvider: ServiceProviderProvider[AuxiliaryData],
              schemaFetcher: SchemaFetcher,
              queryParser: QueryParser,
              queryExecutor: QueryExecutor,
              connectTimeout: FiniteDuration,
              getSchemaTimeout: FiniteDuration,
              queryTimeout: FiniteDuration,
              schemaCache: (String, Option[String], Schema) => Unit,
              schemaDecache: (String, Option[String]) => Option[Schema],
              secondaryInstance: SecondaryInstanceSelector,
              queryRewriter: QueryRewriter,
              rollupInfoFetcher: RollupInfoFetcher)
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
  def unknownColumnIds(columnIds: Set[String]) = BadRequest ~> errContent("req.unknown.column-ids", "columns" -> JsonCodec.toJValue(columnIds.toSeq))
  def rowLimitExceeded(max: BigInt) = BadRequest ~> errContent("req.row-limit-exceeded", "limit" -> JNumber(max))
  def noContentTypeResponse = internalServerError
  def unparsableContentTypeResponse = internalServerError
  def notJsonResponseResponse = internalServerError
  def upstreamTimeoutResponse = GatewayTimeout

  def reqBuilder(secondary: ServiceInstance[AuxiliaryData]) = {
    val pingTarget = for {
      auxData <- Option(secondary.getPayload)
      pingInfo <- auxData.livenessCheckInfo
    } yield pingInfo
    val b = RequestBuilder(secondary.getAddress).
      livenessCheckInfo(pingTarget).
      connectTimeoutMS(connectTimeout.toMillis.toInt)
    if(secondary.getSslPort != null) {
      b.secure(true).port(secondary.getSslPort)
    } else if(secondary.getPort != null) {
      b.port(secondary.getPort)
    } else {
      b
    }
  }

  def storeInCache(schema: Option[Schema], dataset: String, copy: Option[String]): Option[Schema] = schema match {
    case s@Some(trueSchema) =>
      schemaCache(dataset, copy, trueSchema)
      s
    case None =>
      None
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
    Route("/{String}/+", (_: Any, _: Any) => QueryResource),
    Route("/{String}", (_: Any) => QueryResource),
    Route("/version", VersionResource)
  )

  def apply(req: HttpServletRequest) =
    routingTable(req.requestPath) match {
      case Some(resource) => resource(req)
      case None => NotFound
    }

  case class FragmentedQuery(select: Option[String],
                             where: Option[String],
                             group: Option[String],
                             having: Option[String],
                             search: Option[String],
                             order: Option[String],
                             limit: Option[String],
                             offset: Option[String])

  private def process(req: HttpServletRequest)(resp: HttpServletResponse) {
    val originalThreadName = Thread.currentThread.getName
    try {
      Thread.currentThread.setName(Thread.currentThread.getId + " / " + req.getMethod + " " + req.getRequestURI)

      val requestId = RequestId.getFromRequest(req)
      val dataset = Option(req.getParameter("ds")).getOrElse {
        finishRequest(noDatasetResponse)
      }
      val sfDataVersion = req.header("X-SODA2-DataVersion").map(_.toLong).get
      val sfLastModified = req.dateTimeHeader("X-SODA2-LastModified").get

      val forcedSecondaryName = Option(req.getParameter("store"))
      val noRollup = Option(req.getParameter("no_rollup")).isDefined

      forcedSecondaryName.map(ds => log.info("Forcing use of the secondary store instance: " + ds))

      val query = Option(req.getParameter("q")).map(Left(_)).getOrElse {
        Right(FragmentedQuery(
          select = Option(req.getParameter("select")),
          where = Option(req.getParameter("where")),
          group = Option(req.getParameter("group")),
          having = Option(req.getParameter("having")),
          search = Option(req.getParameter("search")),
          order = Option(req.getParameter("order")),
          limit = Option(req.getParameter("limit")),
          offset = Option(req.getParameter("offset"))
        ))
      }

      val rowCount = Option(req.getParameter("rowCount"))
      val copy = Option(req.getParameter("copy"))

      val jsonizedColumnIdMap = Option(req.getParameter("idMap")).getOrElse {
        return (BadRequest ~> Content("no idMap provided"))(resp)
      }
      val columnIdMap: Map[ColumnName, String] = try {
        JsonUtil.parseJson[Map[String,String]](jsonizedColumnIdMap) match {
          case Some(rawMap) =>
            rawMap.map { case (col, typ) => ColumnName(col) -> typ }
          case None =>
            finishRequest(BadRequest ~> Content("idMap not an object whose values are strings"))
        }
      } catch {
        case e: JsonReaderException =>
          finishRequest(BadRequest ~> Content("idMap not parsable as JSON"))
      }
      val precondition = req.precondition
      val ifModifiedSince = req.dateTimeHeader("If-Modified-Since")

      def isInSecondary(name: String): Option[Boolean] = {
        // TODO we should either create a separate less expensive method for checking if a dataset
        // is in a secondary, or we should integrate this into schema caching if and when we
        // build that.
        for {
          instance <- Option(secondaryProvider.provider(name).getInstance())
          base <- Some(reqBuilder(instance))
          result <- schemaFetcher(base.receiveTimeoutMS(getSchemaTimeout.toMillis.toInt), dataset, copy) match {
            case SchemaFetcher.Successful(newSchema, _, _) => Some(true)
            case SchemaFetcher.NoSuchDatasetInSecondary => Some(false)
            case other =>
              log.warn("Unexpected response when fetching schema from secondary: {}", other)
              None
          }
        } yield result
      }

      def secondary(dataset: String, instanceName: Option[String]) = {
        val instance = for {
          name <- instanceName
          instance <- Option(secondaryProvider.provider(name).getInstance())
        } yield instance

        instance.getOrElse {
          instanceName.foreach { n => secondaryInstance.flagError(dataset, n) }
          finishRequest(noSecondaryAvailable(dataset))
        }
      }

      val chosenSecondaryName = forcedSecondaryName.orElse { secondaryInstance.getInstanceName(dataset, isInSecondary) }

      // A little spaghetti never hurt anybody!
      // Ok, so the general flow of this code is:
      //   1. Look up the dataset's schema (in cache or, if
      //     necessary, from the secondary)
      //   2. Analyze the query -- if this fails, and the failure
      //     was a type- or name-related error, and the schema came
      //     from the cache, refresh the schema and try again.
      //   3. Make the actual "give me data" request.
      // That last step is the most complex, because it is a
      // potentially long-running thing, and can cause a retry
      // if the upstream says "the schema just changed".
      val second = secondary(dataset, chosenSecondaryName)
      val base = reqBuilder(second)
      log.info("Base URI: " + base.url)
      def getAndCacheSchema(dataset: String, copy: Option[String]) =
        schemaFetcher(base.receiveTimeoutMS(getSchemaTimeout.toMillis.toInt), dataset, copy) match {
          case SchemaFetcher.Successful(newSchema, _, _) =>
            schemaCache(dataset, copy, newSchema)
            newSchema
          case SchemaFetcher.NoSuchDatasetInSecondary =>
            chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
            finishRequest(notFoundResponse(dataset))
          case SchemaFetcher.TimeoutFromSecondary =>
            chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
            finishRequest(upstreamTimeoutResponse)
          case other =>
            log.error("Unexpected response when fetching schema from secondary: {}", other)
            chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
            finishRequest(internalServerError)
        }

      @tailrec
      def analyzeRequest(schema: Schema, isFresh: Boolean): (Schema, SoQLAnalysis[String, SoQLAnalysisType]) = {
        val parsedQuery = query match {
          case Left(q) =>
            queryParser(q, columnIdMap, schema.schema)
          case Right(fq) =>
            queryParser(
              selection = fq.select,
              where = fq.where,
              groupBy = fq.group,
              having = fq.having,
              orderBy = fq.order,
              limit = fq.limit,
              offset = fq.offset,
              search = fq.search,
              columnIdMapping = columnIdMap,
              schema = schema.schema
            )
        }
        parsedQuery match {
          case QueryParser.SuccessfulParse(analysis) =>
            (schema, analysis)
          case QueryParser.AnalysisError(_: DuplicateAlias | _: NoSuchColumn | _: TypecheckException) if !isFresh =>
            analyzeRequest(getAndCacheSchema(dataset, copy), true)
          case QueryParser.AnalysisError(e) =>
            finishRequest(soqlErrorResponse(dataset, e))
          case QueryParser.UnknownColumnIds(cids) =>
            finishRequest(unknownColumnIds(cids))
          case QueryParser.RowLimitExceeded(max) =>
            finishRequest(rowLimitExceeded(max))
        }
      }

      @tailrec
      def executeQuery(schema: Schema,
                       analyzedQuery: SoQLAnalysis[String, SoQLAnalysisType],
                       rollupName: Option[String],
                       requestId: String,
                       resourceName: Option[String]) {
        val extraHeaders = Map(RequestId.ReqIdHeader -> requestId) ++
                           resourceName.map(fbf => Map("X-Socrata-Resource" -> fbf)).getOrElse(Nil)
        val res = queryExecutor(base.receiveTimeoutMS(queryTimeout.toMillis.toInt),
                                dataset,
                                analyzedQuery,
                                schema,
                                precondition,
                                ifModifiedSince,
                                rowCount,
                                copy,
                                rollupName,
                                extraHeaders).map {
          case QueryExecutor.NotFound =>
            chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
            finishRequest(notFoundResponse(dataset))
          case QueryExecutor.Timeout =>
            // don't flag an error in this case because the timeout may be based on the particular query.
            finishRequest(upstreamTimeoutResponse)
          case QueryExecutor.SchemaHashMismatch(newSchema) =>
            storeInCache(Some(newSchema), dataset, copy)
            val (finalSchema, analysis) = analyzeRequest(newSchema, true)
            val (rewrittenAnalysis, rollupName) = possiblyRewriteQuery(finalSchema, analysis)
            Some((finalSchema, rewrittenAnalysis, rollupName))
          case QueryExecutor.ToForward(responseCode, headers, body) =>
            // Log data version difference if response is OK.  Ignore not modified response and others.
            if (responseCode == HttpStatus.SC_OK) {
              (headers("x-soda2-dataversion").headOption, headers("last-modified").headOption) match {
                case (Some(qsDataVersion), Some(qsLastModified)) =>
                  val qsdv = qsDataVersion.toLong
                  val qslm = HttpUtils.parseHttpDate(qsLastModified)
                  logSchemaFreshness(second.getAddress, sfDataVersion, sfLastModified, qsdv, qslm)
                case _ =>
                  log.warn("version related data not available from secondary")
              }
            }
            resp.setStatus(responseCode)
            for { (h,vs) <- headers; v <- vs } resp.addHeader(h, v)
            transferResponse(resp.getOutputStream, body)
            None
        }
        res match { // bit of a dance because we can't tailrec from within map
          case Some((s, q, r)) => executeQuery(s, q, r, requestId, resourceName)
          case None => // ok
        }
      }

      def possiblyRewriteQuery(schema: Schema, analyzedQuery: SoQLAnalysis[String, SoQLAnalysisType]):
          (SoQLAnalysis[String, SoQLAnalysisType], Option[String]) = {
        if (noRollup) (analyzedQuery, None)
        else {
          rollupInfoFetcher(base.receiveTimeoutMS(getSchemaTimeout.toMillis.toInt), dataset, copy) match {
            case RollupInfoFetcher.Successful(rollups) =>
              val rewritten  = queryRewriter.bestRewrite(schema, analyzedQuery, rollups)
              val (rollupName, analysis) = rewritten map {x => (Some(x._1), x._2) } getOrElse (None, analyzedQuery)
              log.info(s"Rewrote query on dataset ${dataset} to rollup ${rollupName} with analysis ${analysis}")
              (analysis, rollupName)
            case RollupInfoFetcher.NoSuchDatasetInSecondary =>
              chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
              finishRequest(notFoundResponse(dataset))
            case RollupInfoFetcher.TimeoutFromSecondary =>
              chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
              finishRequest(upstreamTimeoutResponse)
            case other =>
              log.error("Unexpected response when fetching schema from secondary: {}", other)
              chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
              finishRequest(internalServerError)
          }
        }
      }

      val (finalSchema, analysis) = schemaDecache(dataset, copy) match {
        case Some(schema) => analyzeRequest(schema, false)
        case None         => analyzeRequest(getAndCacheSchema(dataset, copy), true)
      }
      val (rewrittenAnalysis, rollupName) = possiblyRewriteQuery(finalSchema, analysis)
      executeQuery(finalSchema, rewrittenAnalysis, rollupName,
                   requestId, Option(req.getHeader("X-Socrata-Resource")))
    } catch {
      case FinishRequest(response) =>
      if(resp.isCommitted) ???
      else { resp.reset(); response(resp) }
    } finally {
      Thread.currentThread.setName(originalThreadName)
    }
  }

  private def logSchemaFreshness(secondaryAddress: String,
                                 sfDataVersion: Long,
                                 sfLastModified: DateTime,
                                 qsDataVersion: Long,
                                 qsLastModified: DateTime): Unit = {
    val DataVersionDiff = sfDataVersion - qsDataVersion
    val wrongOrder = qsLastModified.isAfter(sfLastModified)
    val timeDiff = (if (wrongOrder) new Interval(sfLastModified, qsLastModified)
                    else new Interval(qsLastModified, sfLastModified)).toDuration
    if (DataVersionDiff == 0 && timeDiff.getMillis == 0)
      log.info("schema from {} is identical", secondaryAddress)
    else
      log.info("schema from {} differs {} {} {} {} {}v {}{}m",
        secondaryAddress,
        sfDataVersion.toString,
        sfLastModified.toString(ISODateTimeFormat.dateHourMinuteSecond),
        qsDataVersion.toString,
        qsLastModified.toString(ISODateTimeFormat.dateHourMinuteSecond),
        DataVersionDiff.toString,
        (if (wrongOrder) "-" else ""),
        timeDiff.getStandardMinutes.toString)
  }

  private def transferResponse(out: OutputStream, in: InputStream) {
    val buf = new Array[Byte](4096)
    def loop() {
      in.read(buf) match {
        case -1 => // done
        case n => out.write(buf, 0, n); loop()
      }
    }
    loop()
  }
}
