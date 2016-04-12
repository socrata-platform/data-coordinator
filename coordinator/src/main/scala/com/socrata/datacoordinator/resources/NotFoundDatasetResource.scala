package com.socrata.datacoordinator.resources

import java.io.{BufferedWriter, BufferedOutputStream}
import java.nio.charset.StandardCharsets._
import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.io.{CompactJsonWriter, JsonBadParse}
import com.rojoma.json.v3.util.JsonArrayIterator
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.external.BodyRequestError
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.service.ProcessCreationReturns
import com.socrata.datacoordinator.service.ServiceUtil._
import com.socrata.datacoordinator.util.IndexedTempFile
import com.socrata.http.server._
import com.socrata.http.server.responses._
import org.joda.time.format.ISODateTimeFormat
import com.socrata.http.server.implicits._


case class NotFoundDatasetResource(datasetIdRaw: Option[String],
                                   formatDatasetId: DatasetId => String,
                                   tempFileProvider: () => IndexedTempFile,
                                   processCreation: (Iterator[JValue], IndexedTempFile) => ProcessCreationReturns,
                                   datasets: () => Seq[DatasetId],
                                   withMutationScriptResults: (=> HttpResponse) => HttpResponse,
                                   commandReadLimit: Long)
  extends ErrorHandlingSodaResource(formatDatasetId) {


  override val log = org.slf4j.LoggerFactory.getLogger(classOf[NotFoundDatasetResource])
  val dateTimeFormat = ISODateTimeFormat.dateTime

  override def post = {
    datasetIdRaw match {
      case None =>  doCreateDataset
      case Some(datasetId) => _:HttpRequest => notFoundError("")
    }
  }
  override def delete = {
    datasetIdRaw match {
      case None => _:HttpRequest => notFoundError("")
      case Some(datasetId) => _:HttpRequest => notFoundError(datasetId)
    }
  }

  override def get = {
    datasetIdRaw match {
      case None => doListDatasets
      case Some(datasetId) => _:HttpRequest => notFoundError("")
    }
  }


  private def doCreateDataset(req: HttpRequest)(resp: HttpServletResponse) {
    using(tempFileProvider()) { tmp =>
      val responseBuilder = withMutationScriptResults {
        jsonStream(req.servletRequest, commandReadLimit) match {
          case Right((events, boundResetter)) =>
            val iteratorOrError = try {
              Right(JsonArrayIterator.fromEvents[JValue](events))
            } catch { case _: JsonBadParse =>
              Left(datasetErrorResponse(BadRequest, BodyRequestError.NOT_JSON_ARRAY))
            }
            iteratorOrError match {
              case Right(iterator) =>
                val ProcessCreationReturns(dataset, dataVersion, lastModified, result) =
                  processCreation(iterator.map { ev => boundResetter(); ev }, tmp)
                OK ~>
                  Header("X-SODA2-Truth-Last-Modified", dateTimeFormat.print(lastModified)) ~>
                  Header("X-SODA2-Truth-Version", dataVersion.toString) ~>
                  ContentType(JsonContentType) ~>
                  Stream { w =>
                    val bw = new BufferedOutputStream(w)
                    bw.write('[')
                    bw.write(JString(formatDatasetId(dataset)).toString.getBytes(UTF_8))
                    for(r <- result) {
                      bw.write(',')
                      writeResult(bw, r, tmp)
                    }
                    bw.write(']')
                    bw.flush()
                    log.debug("Non-linear index seeks: {}", tmp.stats.nonLinearIndexSeeks)
                    log.debug("Non-linear data seeks: {}", tmp.stats.nonLinearDataSeeks)
                  }
              case Left(error) => error
            }
          case Left(response) => response
        }
      }
      responseBuilder(resp)
    }
  }

  private def doListDatasets(req: HttpRequest): HttpResponse = {
    val ds = datasets()
    OK ~> Write(JsonContentType) { w =>
      val bw = new BufferedWriter(w)
      val jw = new CompactJsonWriter(bw)
      bw.write('[')
      var didOne = false
      for(dsid <- ds) {
        if(didOne) bw.write(',')
        else didOne = true
        jw.write(JString(formatDatasetId(dsid)))
      }
      bw.write(']')
      bw.flush()
    }
  }
}
