package com.socrata.querycoordinator.resources

import java.io.{InputStream, OutputStream}
import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.io.JsonReaderException
import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.common.util.HttpUtils
import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.http.server.util.RequestId
import com.socrata.querycoordinator._
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.exceptions.{TypecheckException, NoSuchColumn, DuplicateAlias}
import com.socrata.soql.types.SoQLAnalysisType
import org.apache.http.HttpStatus
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{Interval, DateTime}
import com.socrata.http.server.implicits._


import scala.concurrent.duration.FiniteDuration


class QueryResource(secondary: Secondary,
                    schemaFetcher: SchemaFetcher,
                    queryParser: QueryParser,
                    queryExecutor: QueryExecutor,
                    schemaTimeout: FiniteDuration,
                    queryTimeout: FiniteDuration,
                    schemaCache: (String, Option[String], Schema) => Unit,
                    schemaDecache: (String, Option[String]) => Option[Schema],
                    secondaryInstance: SecondaryInstanceSelector,
                    queryRewriter: QueryRewriter,
                    rollupInfoFetcher: RollupInfoFetcher) extends QCResource with QueryService { // scalastyle:ignore

  override val log = org.slf4j.LoggerFactory.getLogger(classOf[QueryResource])


  override def get: HttpRequest => HttpServletResponse => Unit = {
    post
  }

  override def put: HttpRequest => HttpServletResponse => Unit = {
    post
  }

  override def post: HttpRequest => HttpServletResponse => Unit = {
    req: HttpRequest => resp: HttpServletResponse =>
      process(req)(resp)
  }


  private def process(req: HttpRequest): HttpResponse = { // scalastyle:ignore cyclomatic.complexity method.length
    val originalThreadName = Thread.currentThread.getName
    val servReq = req.servletRequest
    try {
      Thread.currentThread.setName(Thread.currentThread.getId + " / " + req.method + " " +
        req.servletRequest.getRequestURI)

      val requestId = RequestId.getFromRequest(servReq)
      val dataset = Option(servReq.getParameter("ds")).getOrElse {
        finishRequest(noDatasetResponse)
      }
      val sfDataVersion = req.header("X-SODA2-DataVersion").map(_.toLong).get
      val sfLastModified = req.dateTimeHeader("X-SODA2-LastModified").get

      val forcedSecondaryName = req.queryParameter("store")
      val noRollup = req.queryParameter("no_rollup").isDefined
      val obfuscateId = !Option(servReq.getParameter(qpObfuscateId)).exists(_ == "false")

      forcedSecondaryName.foreach(ds => log.info("Forcing use of the secondary store instance: " + ds))

      val query = Option(servReq.getParameter("q")).map(Left(_)).getOrElse {
        Right(FragmentedQuery(
          select = Option(servReq.getParameter("select")),
          where = Option(servReq.getParameter("where")),
          group = Option(servReq.getParameter("group")),
          having = Option(servReq.getParameter("having")),
          search = Option(servReq.getParameter("search")),
          order = Option(servReq.getParameter("order")),
          limit = Option(servReq.getParameter(qpLimit)),
          offset = Option(servReq.getParameter("offset"))
        ))
      }

      val rowCount = Option(servReq.getParameter("rowCount"))
      val copy = Option(servReq.getParameter("copy"))

      val jsonizedColumnIdMap = Option(servReq.getParameter("idMap")).getOrElse {
        finishRequest(BadRequest ~> Json("no idMap provided"))
      }
      val columnIdMap: Map[ColumnName, String] = try {
        JsonUtil.parseJson[Map[String, String]](jsonizedColumnIdMap) match {
          case Right(rawMap) =>
            rawMap.map { case (col, typ) => ColumnName(col) -> typ }
          case Left(_) =>
            finishRequest(BadRequest ~> Json("idMap not an object whose values are strings"))
        }
      } catch {
        case e: JsonReaderException =>
          finishRequest(BadRequest ~> Json("idMap not parsable as JSON"))
      }
      val precondition = req.precondition
      val ifModifiedSince = req.dateTimeHeader("If-Modified-Since")
      val chosenSecondaryName = secondary.chosenSecondaryName(forcedSecondaryName, dataset, copy)


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
      val second = secondary.serviceInstance(dataset, chosenSecondaryName)
      val base = secondary.reqBuilder(second)
      log.info("Base URI: " + base.url)

      @annotation.tailrec
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

      @annotation.tailrec
      def executeQuery(schema: Schema,
                       analyzedQuery: SoQLAnalysis[String, SoQLAnalysisType],
                       rollupName: Option[String],
                       requestId: String,
                       resourceName: Option[String],
                       resourceScope: ResourceScope): HttpResponse = {
        val extraHeaders = Map(RequestId.ReqIdHeader -> requestId) ++
          resourceName.map(fbf => Map(headerSocrataResource -> fbf)).getOrElse(Nil)
        val res = queryExecutor(
          base.receiveTimeoutMS(queryTimeout.toMillis.toInt),
          dataset,
          analyzedQuery,
          schema,
          precondition,
          ifModifiedSince,
          rowCount,
          copy,
          rollupName,
          obfuscateId,
          extraHeaders,
          resourceScope
        ).map {
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
            Left((finalSchema, rewrittenAnalysis, rollupName))
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
            val resp = transferHeaders(Status(responseCode), headers) ~>
              Stream(out => transferResponse(out, body))
            Right(resp)
        }
        res match {
          // bit of a dance because we can't tailrec from within map
          case Left((s, q, r)) => executeQuery(s, q, r, requestId, resourceName, resourceScope)
          case Right(resp) => resp
        }
      }

      def possiblyRewriteQuery(schema: Schema, analyzedQuery: SoQLAnalysis[String, SoQLAnalysisType]):
      (SoQLAnalysis[String, SoQLAnalysisType], Option[String]) = {
        if (noRollup) {
          (analyzedQuery, None)
        } else {
          rollupInfoFetcher(base.receiveTimeoutMS(schemaTimeout.toMillis.toInt), dataset, copy) match {
            case RollupInfoFetcher.Successful(rollups) =>
              val rewritten = RollupScorer.bestRollup(
                queryRewriter.possibleRewrites(schema, analyzedQuery, rollups).toSeq)
              val (rollupName, analysis) = rewritten map { x => (Some(x._1), x._2) } getOrElse ((None, analyzedQuery))
              log.info(s"Rewrote query on dataset $dataset to rollup $rollupName with analysis $analysis")
              (analysis, rollupName)
            case RollupInfoFetcher.NoSuchDatasetInSecondary =>
              chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
              finishRequest(notFoundResponse(dataset))
            case RollupInfoFetcher.TimeoutFromSecondary =>
              chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
              finishRequest(upstreamTimeoutResponse)
            case other: RollupInfoFetcher.Result =>
              log.error(unexpectedError, other)
              chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
              finishRequest(internalServerError)
          }
        }
      }

      def getAndCacheSchema(dataset: String, copy: Option[String]): Schema = {
        schemaFetcher(base.receiveTimeoutMS(schemaTimeout.toMillis.toInt), dataset, copy) match {
          case SchemaFetcher.Successful(newSchema, _, _) =>
            schemaCache(dataset, copy, newSchema)
            newSchema
          case SchemaFetcher.NoSuchDatasetInSecondary =>
            chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
            finishRequest(notFoundResponse(dataset))
          case SchemaFetcher.TimeoutFromSecondary =>
            chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
            finishRequest(upstreamTimeoutResponse)
          case other: SchemaFetcher.Result =>
            log.error(unexpectedError, other)
            chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
            finishRequest(internalServerError)
        }
      }


      val (finalSchema, analysis) = schemaDecache(dataset, copy) match {
        case Some(schema) => analyzeRequest(schema, false)
        case None => analyzeRequest(getAndCacheSchema(dataset, copy), true)
      }
      val (rewrittenAnalysis, rollupName) = possiblyRewriteQuery(finalSchema, analysis)
      executeQuery(finalSchema, rewrittenAnalysis, rollupName,
        requestId, req.header(headerSocrataResource), req.resourceScope)
    } catch {
      case FinishRequest(response) =>
        response ~> resetResponse _
    } finally {
      Thread.currentThread.setName(originalThreadName)
    }


  }


  private def transferResponse(out: OutputStream, in: InputStream): Unit = {
    val buf = new Array[Byte](responseBuffer)
    @annotation.tailrec
    def loop(): Unit = {
      in.read(buf) match {
        case -1 => // done
        case n: Int =>
          out.write(buf, 0, n)
          loop()
      }
    }
    loop()
  }


  private def storeInCache(schema: Option[Schema], dataset: String, copy: Option[String]): Option[Schema] = {
    schema match {
      case s@Some(trueSchema) =>
        schemaCache(dataset, copy, trueSchema)
        s
      case None =>
        None
    }
  }

  private def logSchemaFreshness(secondaryAddress: String,
                                 sfDataVersion: Long,
                                 sfLastModified: DateTime,
                                 qsDataVersion: Long,
                                 qsLastModified: DateTime): Unit = {
    val DataVersionDiff = sfDataVersion - qsDataVersion
    val wrongOrder = qsLastModified.isAfter(sfLastModified)
    val timeDiff = (if (wrongOrder) {
      new Interval(sfLastModified, qsLastModified)
    } else {
      new Interval(qsLastModified, sfLastModified)
    }).toDuration
    if (DataVersionDiff == 0 && timeDiff.getMillis == 0) {
      log.info("schema from {} is identical", secondaryAddress)
    } else {
      log.info("schema from {} differs {} {} {} {} {}v {}{}m",
        secondaryAddress,
        sfDataVersion.toString,
        sfLastModified.toString(ISODateTimeFormat.dateHourMinuteSecond),
        qsDataVersion.toString,
        qsLastModified.toString(ISODateTimeFormat.dateHourMinuteSecond),
        DataVersionDiff.toString,
        if (wrongOrder) qpHyphen else "",
        timeDiff.getStandardMinutes.toString)
    }
  }


  private def transferHeaders(resp: HttpResponse, headers: Map[String, Seq[String]]): HttpResponse = {
    headers.foldLeft(resp) { case (acc, (h: String, vs: Seq[String])) =>
      vs.foldLeft(acc) { (acc2, v: String) =>
        acc2 ~> Header(h, v)
      }
    }
  }

}

object QueryResource {
  def apply(secondary: Secondary, // scalastyle:ignore
            schemaFetcher: SchemaFetcher,
            queryParser: QueryParser,
            queryExecutor: QueryExecutor,
            schemaTimeout: FiniteDuration,
            queryTimeout: FiniteDuration,
            schemaCache: (String, Option[String], Schema) => Unit,
            schemaDecache: (String, Option[String]) => Option[Schema],
            secondaryInstance: SecondaryInstanceSelector,
            queryRewriter: QueryRewriter,
            rollupInfoFetcher: RollupInfoFetcher): QueryResource = {
    new QueryResource(secondary,
      schemaFetcher,
      queryParser,
      queryExecutor,
      schemaTimeout,
      queryTimeout,
      schemaCache,
      schemaDecache,
      secondaryInstance,
      queryRewriter,
      rollupInfoFetcher
    )
  }
}
