package com.socrata.querycoordinator

import com.socrata.http.client.{Response, RequestBuilder, HttpClient}
import com.socrata.soql.AnalysisSerializer
import com.socrata.soql.types.SoQLAnalysisType
import com.rojoma.simplearm.{SimpleArm, Managed}

import QueryExecutor._
import java.io._
import javax.servlet.http.HttpServletResponse
import com.socrata.http.client.exceptions.{LivenessCheckFailed, HttpClientTimeoutException}
import com.socrata.http.server.implicits._
import scala.annotation.tailrec
import com.rojoma.json.io.{FusedBlockJsonEventIterator, JsonReader}
import java.nio.charset.StandardCharsets
import com.rojoma.json.ast.{JObject, JValue}
import com.rojoma.json.codec.JsonCodec
import com.rojoma.simplearm.util._
import com.socrata.querycoordinator.QueryExecutor.ToForward
import com.socrata.soql.SoQLAnalysis
import com.rojoma.json.ast.JString
import com.socrata.querycoordinator.QueryExecutor.SchemaHashMismatch
import com.socrata.querycoordinator.util.TeeToTempInputStream
import com.socrata.http.server.util.{PreconditionRenderer, Precondition}
import org.joda.time.DateTime

class QueryExecutor(httpClient: HttpClient, analysisSerializer: AnalysisSerializer[String, SoQLAnalysisType], teeStreamProvider: InputStream => TeeToTempInputStream) {
  private[this] val log = org.slf4j.LoggerFactory.getLogger(classOf[QueryExecutor])

  private val qpDataset = "dataset"
  private val qpQuery = "query"
  private val qpSchemaHash = "schemaHash"
  private val qpRowCount = "rowCount"
  private val qpCopy = "copy"
  /**
   * @note Reusing the result will re-issue the request to the upstream server.  The serialization of the
   *       analysis will be re-used for each request.
   */
  def apply(base: RequestBuilder, dataset: String, analysis: SoQLAnalysis[String, SoQLAnalysisType], schema: Schema,
    precondition: Precondition, ifModifiedSince: Option[DateTime],
    rowCount: Option[String],
    copy: Option[String]): Managed[Result] = {
    val serializedAnalysis = serializeAnalysis(analysis)
    val params = List(qpDataset -> dataset, qpQuery -> serializedAnalysis, qpSchemaHash -> schema.hash) ++
      rowCount.map(rc => List(qpRowCount -> rc)).getOrElse(Nil) ++
      copy.map(c => List(qpCopy -> c)).getOrElse(Nil)
    val request = base.p("query").addHeaders(PreconditionRenderer(precondition) ++ ifModifiedSince.map("If-Modified-Since" -> _.toHttpDate)).form(params)

    new SimpleArm[Result] {
      def flatMap[A](f: Result => A): A = {
        var handedOffToUser = false
        try {
          for(result <- httpClient.execute(request)) yield {
            val op = result.resultCode match {
              case HttpServletResponse.SC_NOT_FOUND =>
                NotFound
              case HttpServletResponse.SC_CONFLICT =>
                readSchemaHashMismatch(result, result.asInputStream()) match {
                  case Right(newSchema) =>
                    SchemaHashMismatch(newSchema)
                  case Left(newStream) =>
                    try {
                      forward(result, newStream)
                    } finally {
                       newStream.close()
                    }
                }
              case _ =>
                forward(result)
            }
            handedOffToUser = true
            f(op)
          }
        } catch {
          case e: Throwable if handedOffToUser => // bypass all later cases
            throw e
          case e: HttpClientTimeoutException =>
            f(Timeout)
          case e: LivenessCheckFailed =>
            f(Timeout)
        }
      }
    }
  }

  // rawData should be considered invalid after calling this
  private def readSchemaHashMismatch(result: Response, rawData: InputStream): Either[InputStream, Schema] = {
    using(teeStreamProvider(rawData)) { data =>
      def notMismatchResult: Either[InputStream, Schema] = Left(new SequenceInputStream(data.restream(), rawData))
      try {
        val json = JsonReader.fromEvents(new FusedBlockJsonEventIterator(new InputStreamReader(data, StandardCharsets.UTF_8)))
        checkSchemaHashMismatch(json).fold(notMismatchResult)(Right(_))
      } catch {
        case e: Exception =>
          notMismatchResult
      }
    }
  }

  def checkSchemaHashMismatch(json: JValue): Option[Schema] = {
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

  @tailrec
  private def readFully(data: InputStream, buf: Array[Byte], offset: Int = 0): Int = {
    if(offset == buf.length) buf.length
    else data.read(buf, offset, buf.length - offset) match {
      case -1 => offset
      case n => readFully(data, buf, offset + n)
    }
  }

  private def serializeAnalysis(analysis: SoQLAnalysis[String, SoQLAnalysisType]): String = {
    val baos = new java.io.ByteArrayOutputStream
    analysisSerializer(baos, analysis)
    new String(baos.toByteArray, StandardCharsets.ISO_8859_1)
  }

  private def forward(result: Response, data: InputStream): ToForward =
    ToForward(result.resultCode, result.headerNames.iterator.map { h => h -> (result.headers(h) : Seq[String]) }.toMap, data)

  private def forward(result: Response): ToForward =
    forward(result, result.asInputStream())
}

object QueryExecutor {
  sealed abstract class Result
  case object NotFound extends Result
  case object Timeout extends Result
  case class SchemaHashMismatch(newSchema: Schema) extends Result
  case class ToForward(responseCode: Int, headers: Map[String, Seq[String]], body: InputStream) extends Result
}
