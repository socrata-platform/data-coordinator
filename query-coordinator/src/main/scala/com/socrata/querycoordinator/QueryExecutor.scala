package com.socrata.querycoordinator

import java.io._
import java.nio.charset.StandardCharsets
import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.ast.{JObject, JString, JValue}
import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.io.{FusedBlockJsonEventIterator, JsonReader}
import com.rojoma.simplearm.util._
import com.rojoma.simplearm.v2.ResourceScope
import com.rojoma.simplearm.{Managed, SimpleArm}
import com.socrata.http.client.exceptions.{HttpClientTimeoutException, LivenessCheckFailed}
import com.socrata.http.client.{HttpClient, RequestBuilder, Response}
import com.socrata.http.server.implicits._
import com.socrata.http.server.util.{Precondition, PreconditionRenderer}
import com.socrata.querycoordinator.QueryExecutor.{SchemaHashMismatch, ToForward, _}
import com.socrata.querycoordinator.util.TeeToTempInputStream
import com.socrata.soql.types.SoQLAnalysisType
import com.socrata.soql.{AnalysisSerializer, SoQLAnalysis}
import org.joda.time.DateTime

class QueryExecutor(httpClient: HttpClient,
                    analysisSerializer: AnalysisSerializer[String, SoQLAnalysisType],
                    teeStreamProvider: InputStream => TeeToTempInputStream) {

  private val qpDataset = "dataset"
  private val qpQuery = "query"
  private val qpSchemaHash = "schemaHash"
  private val qpRowCount = "rowCount"
  private val qpCopy = "copy"
  private val qpRollupName = "rollupName"
  private val qpObfuscateId = "obfuscateId"

  /**
   * @note Reusing the result will re-issue the request to the upstream server.  The serialization of the
   *       analysis will be re-used for each request.
   */
  def apply(base: RequestBuilder, // scalastyle:ignore parameter.number method.length cyclomatic.complexity
            dataset: String,
            analysis: SoQLAnalysis[String, SoQLAnalysisType],
            schema: Schema,
            precondition: Precondition,
            ifModifiedSince: Option[DateTime],
            rowCount: Option[String],
            copy: Option[String],
            rollupName: Option[String],
            obfuscateId: Boolean,
            extraHeaders: Map[String, String],
            resourceScope: ResourceScope): Managed[Result] = {
    val serializedAnalysis = serializeAnalysis(analysis)
    val params = List(
      qpDataset -> dataset,
      qpQuery -> serializedAnalysis,
      qpSchemaHash -> schema.hash) ++
      rowCount.map(rc => List(qpRowCount -> rc)).getOrElse(Nil) ++
      copy.map(c => List(qpCopy -> c)).getOrElse(Nil) ++
      rollupName.map(c => List(qpRollupName -> c)).getOrElse(Nil) ++
      (if (!obfuscateId) List(qpObfuscateId -> "false" ) else Nil)
    val request = base.p(qpQuery).
      addHeaders(PreconditionRenderer(precondition) ++ ifModifiedSince.map("If-Modified-Since" -> _.toHttpDate)).
      addHeaders(extraHeaders).
      form(params)

    new SimpleArm[Result] {
      def flatMap[A](f: Result => A): A = {
        var handedOffToUser = false
        try {
          val result = resourceScope.open(httpClient.executeUnmanaged(request))
          val op = result.resultCode match {
            case HttpServletResponse.SC_NOT_FOUND => NotFound
            case HttpServletResponse.SC_CONFLICT =>
              readSchemaHashMismatch(result, result.inputStream()) match {
                case Right(newSchema) =>
                  SchemaHashMismatch(newSchema)
                case Left(newStream) => try {
                  forward(result, newStream)
                } finally {
                  newStream.close()
                }
              }
            case _ => forward(result)
          }
          handedOffToUser = true
          f(op)
        } catch {
          case e: Throwable if handedOffToUser => throw e // bypass all later cases
          case e: HttpClientTimeoutException => f(Timeout)
          case e: LivenessCheckFailed => f(Timeout)
        }
      }
    }
  }

  // rawData should be considered invalid after calling this
  private def readSchemaHashMismatch(result: Response, rawData: InputStream): Either[InputStream, Schema] = {
    using(teeStreamProvider(rawData)) { data =>
      def notMismatchResult: Either[InputStream, Schema] = Left(new SequenceInputStream(data.restream(), rawData))
      try {
        val json = JsonReader.fromEvents(
          new FusedBlockJsonEventIterator(new InputStreamReader(data, StandardCharsets.UTF_8)))
        checkSchemaHashMismatch(json).fold(notMismatchResult)(Right(_))
      } catch {
        case e: Exception =>
          notMismatchResult
      }
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

  private def serializeAnalysis(analysis: SoQLAnalysis[String, SoQLAnalysisType]): String = {
    val baos = new java.io.ByteArrayOutputStream
    analysisSerializer(baos, analysis)
    new String(baos.toByteArray, StandardCharsets.ISO_8859_1)
  }

  private def forward(result: Response, data: InputStream): ToForward = ToForward(
    result.resultCode, result.headerNames.iterator.map { h => h -> (result.headers(h): Seq[String]) }.toMap, data)

  private def forward(result: Response): ToForward =
    forward(result, result.inputStream())
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
