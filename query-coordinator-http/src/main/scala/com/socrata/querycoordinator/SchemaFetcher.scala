package com.socrata.querycoordinator

import java.io.IOException

import com.rojoma.json.v3.io.JsonReaderException
import com.rojoma.simplearm.util._
import com.socrata.http.client.{HttpClient, RequestBuilder, Response}
import com.socrata.http.client.exceptions.{HttpClientException, HttpClientTimeoutException, LivenessCheckFailed}
import com.socrata.http.common.util.HttpUtils
import com.socrata.querycoordinator.SchemaFetcher._
import javax.servlet.http.HttpServletResponse
import org.joda.time.DateTime

class SchemaFetcher(httpClient: HttpClient) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[SchemaFetcher])

  def apply(base: RequestBuilder, dataset: String, copy: Option[String]): Result = {
    def processResponse(response: Response): Result = response.resultCode match {
      case HttpServletResponse.SC_OK =>
        val dataVersion = response.headers("X-SODA2-DataVersion")(0).toLong
        val lastModified = HttpUtils.parseHttpDate(response.headers("Last-Modified")(0))
        response.value[Schema]() match {
          case Right(schema) => Successful(schema, dataVersion, lastModified)
          case Left(err) =>
            log.warn("cannot decode schema {}", err)
            NonSchemaResponse
        }
      case HttpServletResponse.SC_NOT_FOUND =>
        NoSuchDatasetInSecondary
      case other =>
        log.error("Unexpected response code {} from request for schema of dataset {} from {}:{}", other.asInstanceOf[AnyRef], dataset.asInstanceOf[AnyRef], base.url)
        BadResponseFromSecondary
    }

    val params = Seq("ds" -> dataset) ++ copy.map(c => Seq("copy" -> c)).getOrElse(Nil)
    val request = base.p("schema").q(params : _*).get

    try {
      httpClient.execute(request).run(processResponse)
    } catch {
      case e: HttpClientTimeoutException =>
        TimeoutFromSecondary
      case e: LivenessCheckFailed =>
        TimeoutFromSecondary
      case e: HttpClientException =>
        BadResponseFromSecondary
      case e: IOException =>
        BadResponseFromSecondary
    }
  }
}

object SchemaFetcher {
  sealed abstract class Result
  case class Successful(schema: Schema, dataVersion: Long, lastModified: DateTime) extends Result
  case object NonSchemaResponse extends Result
  case object NoSuchDatasetInSecondary extends Result
  case object BadResponseFromSecondary extends Result
  case object TimeoutFromSecondary extends Result
}
