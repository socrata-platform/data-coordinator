package com.socrata.querycoordinator

import com.rojoma.json.util.JsonArrayIterator.ElementDecodeException
import com.socrata.http.client.{Response, RequestBuilder, HttpClient}
import javax.servlet.http.HttpServletResponse
import com.rojoma.json.io.JsonReaderException
import com.rojoma.simplearm.util._

import RollupInfoFetcher._
import com.socrata.http.client.exceptions.{LivenessCheckFailed, HttpClientException, HttpClientTimeoutException}
import java.io.IOException

/**
 * Fetches the rollup info from the secondary server.  Modelled after the SchemaFetcher, it would be nice
 * if they weren't separate operations but changing the schema response is a significantly bigger project.
 */
class RollupInfoFetcher(httpClient: HttpClient) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[RollupInfoFetcher])

  def apply(base: RequestBuilder, dataset: String, copy: Option[String]): Result = {
    def processResponse(response: Response): Result = response.resultCode match {
      case HttpServletResponse.SC_OK =>
        try {
          Successful(response.array[RollupInfo]().toList)
        } catch {
          case e @ (_ : JsonReaderException | _ : ElementDecodeException) =>
            NonRollupInfoResponse
        }
      case HttpServletResponse.SC_NOT_FOUND =>
        NoSuchDatasetInSecondary
      case other =>
        log.error("Unexpected response code {} from request for rollup info of dataset {} from {}:{}", other.asInstanceOf[AnyRef], dataset.asInstanceOf[AnyRef], base.url)
        BadResponseFromSecondary
    }

    val params = Seq("ds" -> dataset) ++ copy.map(c => Seq("copy" -> c)).getOrElse(Nil)
    val request = base.p("rollups").q(params: _*).get

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

object RollupInfoFetcher {
  sealed abstract class Result
  case class Successful(rollupInfos: Seq[RollupInfo]) extends Result
  case object NonRollupInfoResponse extends Result
  /** Not found on the secondary we asked, should be rare as secondary discovery takes care of
    * making sure we are asking a secondary that has the dataset.
    */
  case object NoSuchDatasetInSecondary extends Result
  case object BadResponseFromSecondary extends Result
  case object TimeoutFromSecondary extends Result
}
