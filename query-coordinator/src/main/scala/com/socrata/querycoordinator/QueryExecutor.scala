package com.socrata.querycoordinator

import java.io._
import java.nio.charset.StandardCharsets
import java.util.concurrent.Semaphore
import javax.servlet.http.HttpServletResponse
import com.socrata.http.common.util.HttpUtils
import com.socrata.querycoordinator.caching.cache.noop.NoopCacheSessionProvider
import com.socrata.querycoordinator.caching.{Windower, SoQLAnalysisDepositioner, Hasher}

import scala.collection.JavaConverters._

import com.rojoma.json.v3.ast.{JObject, JString, JValue}
import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.io.{CompactJsonWriter, FusedBlockJsonEventIterator, JsonReader}
import com.rojoma.json.v3.util.{JsonArrayIterator, AutomaticJsonCodecBuilder, ArrayIteratorEncode, JsonUtil}
import com.rojoma.simplearm.v2._
import com.socrata.http.client.exceptions.{HttpClientTimeoutException, LivenessCheckFailed}
import com.socrata.http.client.{HttpClient, RequestBuilder, Response}
import com.socrata.http.server.implicits._
import com.socrata.http.server.util._
import com.socrata.querycoordinator.QueryExecutor.{SchemaHashMismatch, ToForward, _}
import com.socrata.querycoordinator.util.TeeToTempInputStream
import com.socrata.querycoordinator.caching.cache.{CacheSession, ValueRef, CacheSessionProvider}
import com.socrata.querycoordinator.caching.SharedHandle
import com.socrata.util.io.SplitStream
import com.socrata.soql.types.SoQLAnalysisType
import com.socrata.soql.{AnalysisSerializer, SoQLAnalysis}
import org.joda.time.DateTime

import scala.annotation.tailrec
import scala.util.control.ControlThrowable

class QueryExecutor(httpClient: HttpClient,
                    analysisSerializer: AnalysisSerializer[String, SoQLAnalysisType],
                    teeStreamProvider: InputStream => TeeToTempInputStream,
                    cacheSessionProvider: CacheSessionProvider,
                    windower: Windower,
                    maxWindowsToCache: BigInt) {
  private val forceCacheEvenWhenNoop = java.lang.Boolean.getBoolean("com.socrata.query-coordinator.force-cache-even-when-noop")

  private val qpDataset = "dataset"
  private val qpQuery = "query"
  private val qpSchemaHash = "schemaHash"
  private val qpRowCount = "rowCount"
  private val qpCopy = "copy"
  private val qpRollupName = "rollupName"
  private val qpObfuscateId = "obfuscateId"

  private case class Headers(http: Map[String, Seq[String]], cjson: JValue)
  private implicit val hCodec = AutomaticJsonCodecBuilder[Headers]

  private def makeCacheKeyBase(dsId: String,
                               query: Seq[SoQLAnalysis[String, SoQLAnalysisType]],
                               schemaHash: String,
                               lastModified: DateTime,
                               copyNum: Long,
                               dataVer: Long,
                               rollupName: Option[String],
                               obfuscateId: Boolean,
                               rowCount: Option[String]): String = {
    val depositionedQuery = query.map(SoQLAnalysisDepositioner(_))
    hexString(Hasher.hash(dsId, serializeAnalysis(depositionedQuery),
      schemaHash, lastModified.getMillis, copyNum, dataVer, rollupName, if (obfuscateId) 1L else 0L, rowCount))
  }

  private def hexString(bs: Array[Byte]) = bs.map("%02x".format(_)).mkString
  private def cacheKey(base: String, remainder: String): String = base + "." + remainder

  /**
   * @note Reusing the result will re-issue the request to the upstream server.  The serialization of the
   *       analysis will be re-used for each request.
   *
   * @note If this returns ToForward, the input stream will be managed by the given ResourceScope.
   */
  def apply(base: RequestBuilder, // scalastyle:ignore parameter.number method.length cyclomatic.complexity
            dataset: String,
            analyses: Seq[SoQLAnalysis[String, SoQLAnalysisType]],
            schema: Schema,
            precondition: Precondition,
            ifModifiedSince: Option[DateTime],
            rowCount: Option[String],
            copy: Option[String],
            rollupName: Option[String],
            obfuscateId: Boolean,
            extraHeaders: Map[String, String],
            currentCopyNumber: Long,
            currentDataVersion: Long,
            currentLastModified: DateTime,
            resourceScopeHandle: SharedHandle[ResourceScope]): Result = {
    val rs = resourceScopeHandle.get
    def go(theAnalyses: Seq[SoQLAnalysis[String, SoQLAnalysisType]] = analyses): Result =
      reallyApply(base, dataset, theAnalyses, schema, precondition, ifModifiedSince, rowCount, copy, rollupName, obfuscateId, extraHeaders, rs)

    if(cacheSessionProvider == NoopCacheSessionProvider && !forceCacheEvenWhenNoop) return go()
    // checking preconditions will be handled below
    if(analyses.last.limit.isEmpty) return go()

    val origLimit = analyses.last.limit.get
    val origOffset = analyses.last.offset.getOrElse(BigInt(0))
    val (startWindow, endWindow) = windower(origLimit, origOffset)
    val newOffset = startWindow.window * windower.windowSize
    val newLimit = ((endWindow.window + 1) * windower.windowSize) - newOffset
    val totalWindows = endWindow.window - startWindow.window + 1
    if(totalWindows > maxWindowsToCache) return go()

    val unlimitedAnalyses = analyses.dropRight(1) :+
      analyses.last.copy[String, SoQLAnalysisType](limit = None, offset = None)
    val readCacheKeyBase = makeCacheKeyBase(dataset, unlimitedAnalyses, schema.hash, currentLastModified, currentCopyNumber, currentDataVersion, rollupName, obfuscateId, rowCount)

    using(new ResourceScope()) { tmpScope =>
      val cacheSession = cacheSessionProvider.open(rs)

      // ok, here we get a little ugly.  And by a little ugly, I mean "a lot ugly".  So this
      // thing is trying to read data out of the cache.  If it's not found, it will fall back to
      // just making the request in and caching its result.

      def tryToServeFromCache(): Option[Result] = {
        val headers = cacheSession.find(cacheKey(readCacheKeyBase, "headers"), rs) match {
          case None =>
            return None
          case Some(headerVal) =>
            val r = headerVal.openText(rs)
            val header = JsonUtil.readJson[Headers](r)
            rs.close(r)
            rs.close(headerVal)
            header.right.getOrElse(return None)
        }

        // ok, we have the headers from last time.  If the preconditions _fail_, we'll pass the request
        // to the real secondary so we don't have to mock up identical responses.  (It is just vaguely
        // possible that those requests will still succeed, but if so... ok, such is life.)
        val etag = headers.http.get("etag").flatMap(_.headOption).map(EntityTagParser.parse(_))
        if(precondition.check(etag, sideEffectFree = true) != Precondition.Passed) return Some(go())
        val lastModified = headers.http.get("last-modified").flatMap(_.headOption)
        if(ifModifiedSince.map(HttpUtils.HttpDateFormat.print) == lastModified) return Some(go())

        def isEnd(vr: ValueRef) = {
          val is = vr.open(rs)
          val empty = is.read() == -1
          rs.close(is)
          empty
        }

        @tailrec
        def lookupWindows(acc: List[ValueRef], i: BigInt): Option[Seq[ValueRef]] = {
          if(i <= endWindow.window) {
            cacheSession.find(cacheKey(readCacheKeyBase, i.toString), rs) match {
              case Some(vr) =>
                if(isEnd(vr)) { rs.close(vr); Some(acc.reverse) }
                else lookupWindows(vr :: acc, i + 1)
              case None =>
                acc.foreach(rs.close(_))
                None
            }
          } else Some(acc.reverse)
        }

        def parseWindow(v: ValueRef) = parseWindowInScope(v, rs)

        val values: Iterator[JValue] =
          lookupWindows(Nil, startWindow.window) match {
            case None =>
              return None
            case Some(Nil) =>
              Iterator.empty
            case Some(singleWindow :: Nil) =>
              parseWindow(singleWindow).take(endWindow.index).drop(startWindow.index)
            case Some(firstWindow :: moreWindows) =>
              val middleWindows = moreWindows.dropRight(1)
              val lastWindow = moreWindows.last
              parseWindow(firstWindow).drop(startWindow.index) ++ middleWindows.flatMap(parseWindow) ++ parseWindow(lastWindow).take(endWindow.index)
          }

        log.info("Serving response from cache!")
        Some(ToForward(200, headers.http, rs.openUnmanaged(reserialize(Iterator.single(headers.cjson) ++ values))))
      }

      tryToServeFromCache() match {
        case Some(result) =>
          result
        case None =>
          log.info("Not in cache!")
          val relimitedAnalyses = analyses.dropRight(1) :+
            analyses.last.copy[String, SoQLAnalysisType](limit = Some(newLimit), offset = Some(newOffset))
          go(theAnalyses = relimitedAnalyses) match {
            case ToForward(200, headers0, body) =>
              val headers = headers0 - "content-length" // we'll be manipulating the values, so remove that if it's set

              val (forForward, forCache) = SplitStream(body, 1024*1024, rs, transitiveCloseIn = false)
              val ready = new Semaphore(0)
              val cache = new Thread {
                setDaemon(true)
                setName("Cache thread")

                override def run(): Unit = {
                  using(resourceScopeHandle.duplicate()) { handle =>
                    ready.release()
                    doCache(dataset, unlimitedAnalyses, schema, rollupName, obfuscateId, rowCount, headers, forCache, handle.get, cacheSession, startWindow.window, endWindow.window)
                  }
                }
              }
              cache.start()
              ready.acquire()

              val jvalues = JsonArrayIterator[JValue](new FusedBlockJsonEventIterator(new InputStreamReader(forForward, StandardCharsets.UTF_8)))
              val cjsonHeader = jvalues.next()

              val interestingSubset = takeBigInt(dropBigInt(jvalues, origOffset - newOffset), origLimit)

              ToForward(200, headers, rs.openUnmanaged(reserialize(Iterator.single(cjsonHeader) ++ interestingSubset), transitiveClose = List(forForward)))
            case other =>
              other
          }
      }
    }
  }

  private def parseWindowInScope(window: ValueRef, rs: ResourceScope): Iterator[JValue] = new Iterator[JValue] {
    val stream = window.openText(rs)
    val underlying = JsonArrayIterator[JValue](new FusedBlockJsonEventIterator(stream))
    var seenEOF = false
    def hasNext = {
      if(!seenEOF && !underlying.hasNext) { seenEOF = true; rs.close(stream); rs.close(window) }
      !seenEOF
    }
    def next(): JValue = {
      if(!hasNext) Iterator.empty.next()
      underlying.next()
    }
  }

  private def doCache(dataset: String,
                      unlimitedAnalyses: Seq[SoQLAnalysis[String, SoQLAnalysisType]],
                      schema: Schema,
                      rollupName: Option[String],
                      obfuscateId: Boolean,
                      rowCount: Option[String],
                      httpHeaders: Map[String, Seq[String]],
                      body: InputStream,
                      rs: ResourceScope,
                      cacheSession: CacheSession,
                      startWindow: BigInt,
                      endWindow: BigInt): Unit = {
    val abort = { (why: String) => log.warn(why); return } // this "return" needs to return from doCache, not abort.  Thus val not def
    val jvalues = JsonArrayIterator[JValue](new FusedBlockJsonEventIterator(new InputStreamReader(body, StandardCharsets.UTF_8)))
    val cjsonHeader = jvalues.next()
    // ok, we'll need to make a new cache key based on the headers
    val headers = Headers(httpHeaders, cjsonHeader)
    val lastModified = httpHeaders.get("last-modified").map { xs =>
      HttpUtils.parseHttpDate(xs.head)
    }.getOrElse(abort("No last-modified in the response"))
    val copyNumber = httpHeaders.get("x-soda2-copynumber").map(_.head.toLong).getOrElse(abort("No copy number in the response"))
    val dataVersion = httpHeaders.get("x-soda2-dataversion").map(_.head.toLong).getOrElse(abort("No data version in the response"))
    val cacheKeyBase = makeCacheKeyBase(dataset, unlimitedAnalyses, schema.hash, lastModified, copyNumber, dataVersion, rollupName, obfuscateId, rowCount)
    cacheSession.createText(cacheKey(cacheKeyBase, "headers")) { out =>
      JsonUtil.writeJson(out, headers)
    }
    var nextWindowNum = startWindow
    while(jvalues.hasNext && nextWindowNum <= endWindow) {
      val values = take(jvalues, windower.windowSize)
      if(values.nonEmpty) {
        cacheSession.createText(cacheKey(cacheKeyBase, nextWindowNum.toString)) { out =>
          JsonUtil.writeJson(out, values)
        }
      }
      nextWindowNum += 1
    }
    if(nextWindowNum <= endWindow) {
      cacheSession.createText(cacheKey(cacheKeyBase, nextWindowNum.toString)) { out =>
        // write nothing
      }
    }
    rs.close(body)
  }

  // This does NOT invalidate the input iterator
  private def take[T](in: Iterator[T], n: Int): Seq[T] = {
    val result = Seq.newBuilder[T]
    var remaining = n
    while(remaining > 0 && in.hasNext) {
      result += in.next()
      remaining -= 1
    }
    result.result()
  }

  @tailrec
  private def dropBigInt[T](in: Iterator[T], x: BigInt): Iterator[T] = {
    if(x <= 0 || !in.hasNext) in
    else { in.next(); dropBigInt(in, x - 1) }
  }

  private def takeBigInt[T](in: Iterator[T], x: BigInt): Iterator[T] = new Iterator[T] {
    var remaining = x
    def hasNext = remaining > 0 && in.hasNext
    def next() = {
      if(!hasNext) Iterator.empty.next()
      else {
        remaining -= 1
        in.next()
      }
    }
  }

  private def reserialize(jvalues: Iterator[JValue]): InputStream = {
    new SequenceInputStream(ArrayIteratorEncode.toText(jvalues).map { s => new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)) }.asJavaEnumeration)
  }

  private def reallyApply(base: RequestBuilder, // scalastyle:ignore parameter.number method.length cyclomatic.complexity
                          dataset: String,
                          analyses: Seq[SoQLAnalysis[String, SoQLAnalysisType]],
                          schema: Schema,
                          precondition: Precondition,
                          ifModifiedSince: Option[DateTime],
                          rowCount: Option[String],
                          copy: Option[String],
                          rollupName: Option[String],
                          obfuscateId: Boolean,
                          extraHeaders: Map[String, String],
                          resourceScope: ResourceScope): Result = {
    val serializedAnalyses = serializeAnalysis(analyses)
    val params = List(
      qpDataset -> dataset,
      qpQuery -> serializedAnalyses,
      qpSchemaHash -> schema.hash) ++
      rowCount.map(rc => List(qpRowCount -> rc)).getOrElse(Nil) ++
      copy.map(c => List(qpCopy -> c)).getOrElse(Nil) ++
      rollupName.map(c => List(qpRollupName -> c)).getOrElse(Nil) ++
      (if (!obfuscateId) List(qpObfuscateId -> "false" ) else Nil)
    val request = base.p(qpQuery).
      addHeaders(PreconditionRenderer(precondition) ++ ifModifiedSince.map("If-Modified-Since" -> _.toHttpDate)).
      addHeaders(extraHeaders).
      form(params)

    try {
      val result = httpClient.execute(request, resourceScope)
      result.resultCode match {
        case HttpServletResponse.SC_NOT_FOUND =>
          resourceScope.close(result)
          NotFound
        case HttpServletResponse.SC_CONFLICT =>
          readSchemaHashMismatch(result, resourceScope) match {
            case Right(newSchema) =>
              resourceScope.close(result)
              SchemaHashMismatch(newSchema)
            case Left(newStream) => try {
              forward(result, newStream)
            } finally {
              newStream.close()
            }
          }
        case _ =>
          forward(result, resourceScope.openUnmanaged(result.inputStream(), transitiveClose = List(result)))
      }
    } catch {
      case e: HttpClientTimeoutException => Timeout
      case e: LivenessCheckFailed => Timeout
    }
  }

  // rawData should be considered invalid after calling this
  // If this returns a Left, the contained InputStream will be managed
  // by the resourceScope, and have a transitive close dependency on the
  // Response.  If it returns a Right, the input Response will NOT be closed.
  private def readSchemaHashMismatch(result: Response, resourceScope: ResourceScope): Either[InputStream, Schema] = {
    val rawData = result.inputStream()
    val data = resourceScope.open(teeStreamProvider(rawData), transitiveClose = List(result))
    def notMismatchResult: Either[InputStream, Schema] = Left(resourceScope.open(new SequenceInputStream(data.restream(), rawData), transitiveClose = List(data)))
    try {
      val json = JsonReader.fromEvents(
        new FusedBlockJsonEventIterator(new InputStreamReader(data, StandardCharsets.UTF_8)))
      checkSchemaHashMismatch(json) match {
        case Some(schema) =>
          Right(schema)
        case None =>
          notMismatchResult
      }
    } catch {
      case e: Exception =>
        notMismatchResult
    }
  }

  @annotation.tailrec
  private def readFully(data: InputStream, buf: Array[Byte], offset: Int = 0): Int = {
    if (offset == buf.length) {
      buf.length
    } else {
      data.read(buf, offset, buf.length - offset) match {
        case -1 => offset
        case n: Int => readFully(data, buf, offset + n)
      }
    }
  }

  private def serializeAnalysis(analysis: Seq[SoQLAnalysis[String, SoQLAnalysisType]]): String = {
    val baos = new java.io.ByteArrayOutputStream
    analysisSerializer(baos, analysis)
    new String(baos.toByteArray, StandardCharsets.ISO_8859_1)
  }

  private def forward(result: Response, data: InputStream): ToForward = ToForward(
    result.resultCode, result.headerNames.iterator.map { h => h -> (result.headers(h): Seq[String]) }.toMap, data)
}

object QueryExecutor {

  private val log = org.slf4j.LoggerFactory.getLogger(classOf[QueryExecutor])

  sealed abstract class Result

  case object NotFound extends Result

  case object Timeout extends Result

  case class SchemaHashMismatch(newSchema: Schema) extends Result

  case class ToForward(responseCode: Int, headers: Map[String, Seq[String]], body: InputStream) extends Result

  def checkSchemaHashMismatch(json: JValue): Option[Schema] = {
    for {
      obj: JObject <- json.cast[JObject].orElse { log.error("Response is not a JSON object"); None }
      errorCode: JValue <- obj.get("errorCode").orElse { log.error("Response is missing errorCode field"); None }
      errorCodeSchemaMismatch <- if (errorCode == JString("internal.schema-mismatch")) Some(1) else None
      data: JValue <- obj.get("data").orElse { log.error("Response does not contain a data field"); None }
      schema: Schema <- JsonDecode[Schema].decode(data) match {
        case Left(decodeError) => log.error("data object is not a valid Schema"); None
        case Right(schema) => Some(schema)
      }
    } yield schema
  }
}
