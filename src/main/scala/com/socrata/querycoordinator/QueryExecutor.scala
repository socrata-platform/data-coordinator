package com.socrata.querycoordinator

import com.socrata.http.client.{Response, RequestBuilder, HttpClient}
import com.socrata.soql.{SoQLAnalysis, AnalysisSerializer}
import com.socrata.soql.types.SoQLAnalysisType
import com.rojoma.simplearm.{SimpleArm, Managed}

import QueryExecutor._
import java.io.{ByteArrayInputStream, InputStreamReader, BufferedInputStream, InputStream}
import javax.servlet.http.HttpServletResponse
import com.socrata.http.client.exceptions.{LivenessCheckFailed, HttpClientTimeoutException}
import scala.annotation.tailrec
import com.rojoma.json.io.{FusedBlockJsonEventIterator, JsonReader}
import java.nio.charset.StandardCharsets
import com.rojoma.json.ast.{JString, JObject, JValue}
import com.rojoma.json.codec.JsonCodec

class QueryExecutor(httpClient: HttpClient, analysisSerializer: AnalysisSerializer[String, SoQLAnalysisType]) {
  private[this] val log = org.slf4j.LoggerFactory.getLogger(classOf[QueryExecutor])

  /**
   * @note Reusing the result will re-issue the request to the upstream server.  The serialization of the
   *       analysis will be re-used for each request.
   */
  def apply(base: RequestBuilder, dataset: String, analysis: SoQLAnalysis[String, SoQLAnalysisType], schema: Schema, ifNoneMatch: Option[String]): Managed[Result] = {
    val serializedAnalysis = serializeAnalysis(analysis)
    val request = ifNoneMatch.foldLeft(base.p("query"))(_.addHeader("If-None-Match", _)).form(Map(
      "dataset" -> dataset,
      "query" -> serializedAnalysis,
      "schemaHash" -> schema.hash
    ))

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
                  case Right(newSchema) => SchemaHashMismatch(newSchema)
                  case Left(newStream) => forward(result, newStream)
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

  private def readSchemaHashMismatch(result: Response, rawData: InputStream): Either[InputStream, Schema] = {
    // FIXME: this is only correct if a "schema mismatch" error is GUARANTEED to fit in 9K.
    // Which it isn't, in general.  What this SHOULD do is use the JSON stream-processing stuff
    // and a temp buffer, returning an input stream which is a SequenceInputStream formed
    // from the temp buffer and the remaining input.
    val data = new BufferedInputStream(rawData)
    data.mark(10240)
    val bytes = new Array[Byte](1024 * 9)
    val endptr = readFully(data, bytes)
    data.reset()
    val notMismatchResult: Either[InputStream, Schema] = Left(data)

    try {
      val json = JsonReader.fromEvents(new FusedBlockJsonEventIterator(new InputStreamReader(new ByteArrayInputStream(bytes, 0, endptr), StandardCharsets.UTF_8)))
      checkSchemaHashMismatch(json).fold(notMismatchResult)(Right(_))
    } catch {
      case e: Exception =>
        notMismatchResult
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
    new String(baos.toByteArray, "latin1")
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
