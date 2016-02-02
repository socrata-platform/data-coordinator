package com.socrata.datacoordinator.resources

import java.io.{BufferedWriter, BufferedOutputStream}
import java.nio.charset.StandardCharsets._
import java.security.MessageDigest
import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.ast.{JArray, JString, JNumber, JValue}
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.io.{CompactJsonWriter, JsonBadParse}
import com.rojoma.json.v3.util.JsonArrayIterator
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.id.{UserColumnId, DatasetId}
import com.socrata.datacoordinator.service.ServiceUtil._
import com.socrata.datacoordinator.service._
import com.socrata.datacoordinator.truth._
import com.socrata.datacoordinator.truth.loader.DatasetDropper
import com.socrata.datacoordinator.truth.metadata.{SchemaField, Schema}
import com.socrata.datacoordinator.util.IndexedTempFile
import com.socrata.datacoordinator.util.collection.UserColumnIdSet
import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.http.server.util.{EntityTag, Precondition}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import com.socrata.http.server.implicits._
import com.socrata.datacoordinator.common.util.DatasetIdNormalizer._
import com.socrata.datacoordinator.external._



case class DatasetResource(datasetId: DatasetId,
                           tempFileProvider: () => IndexedTempFile,
                           commandReadLimit: Long,
                           processMutation: (DatasetId, Iterator[JValue], IndexedTempFile) => ProcessMutationReturns,
                           deleteDataset: DatasetId => DatasetDropper.Result,
                           datasetContents: (DatasetId, Option[String], CopySelector, Option[UserColumnIdSet],
                             Option[Long], Option[Long], Precondition, Option[DateTime], Boolean, Option[String]) =>
                             DatasetResource.datasetContentsFunc => Exporter.Result[Unit],
                           withMutationScriptResults: (=> HttpResponse) => HttpResponse,
                           formatDatasetId: DatasetId => String) extends ErrorHandlingSodaResource(formatDatasetId) {
  override val log = org.slf4j.LoggerFactory.getLogger(classOf[DatasetResource])

  val dateTimeFormat = ISODateTimeFormat.dateTime
  val mutateRate = metrics.meter("mutation-rate", "rows")
  val suffixHashAlg = "SHA1"
  val suffixHashLen = MessageDigest.getInstance(suffixHashAlg).getDigestLength

  val COPY_LATEST = "latest"
  val COPY_PUBLISHED = "published"
  val COPY_WORKING = "working"

  val HEADER_LAST_MODIFIED = "X-SODA2-Truth-Last-Modified"
  val HEADER_TRUTH_VERSION = "X-SODA2-Truth-Version"
  val HEADER_TRUTH_COPY_NUMBER = "X-SODA2-Truth-Copy-Number"

  override def post = doMutation
  override def delete = doDeleteDataset
  override def get = doExportFile

  private def doMutation(req: HttpRequest)(resp: HttpServletResponse) = {
    using(tempFileProvider()) { tmp =>
      val responseBuilder = withMutationScriptResults {
        jsonStream(req.servletRequest, commandReadLimit) match {
          case Right((events, boundResetter)) =>
            val iteratorOrError = try {
              Right(JsonArrayIterator[JValue](events))
            } catch { case _: JsonBadParse =>
              Left(datasetBadRequest(BodyRequestError.NOT_JSON_ARRAY))
            }
            iteratorOrError match {
              case Right(iterator) =>
                val ProcessMutationReturns(copyNumber, version, lastModified, result) =
                  processMutation(datasetId, iterator.map { ev => boundResetter(); mutateRate.mark(); ev }, tmp)
                OK ~>
                  ContentType(JsonContentType) ~>
                  Header(HEADER_LAST_MODIFIED, dateTimeFormat.print(lastModified)) ~>
                  Header(HEADER_TRUTH_VERSION, version.toString) ~>
                  Header(HEADER_TRUTH_COPY_NUMBER, copyNumber.toString) ~>
                  Stream { w =>
                    val bw = new BufferedOutputStream(w)
                    bw.write('[')
                    result.foreach(new Function1[MutationScriptCommandResult, Unit] {
                      var didOne = false
                      def apply(r: MutationScriptCommandResult) {
                        if(didOne) bw.write(',')
                        else didOne = true
                        writeResult(bw, r, tmp)
                      }
                    })
                    bw.write(']')
                    bw.flush()

                    log.debug("Non-linear index seeks: {}", tmp.stats.nonLinearIndexSeeks)
                    log.debug("Non-linear data seeks: {}", tmp.stats.nonLinearDataSeeks)
                  }
              case Left(error) =>
                error
            }
          case Left(response) =>
            response
        }
      }
      responseBuilder(resp)
    }
  }

  private def doDeleteDataset(req: HttpRequest): HttpResponse = {
    deleteDataset(datasetId) match {
      case DatasetDropper.Success =>
        OK ~>
          Header(HEADER_LAST_MODIFIED, dateTimeFormat.print(DateTime.now)) ~>
          Header(HEADER_TRUTH_COPY_NUMBER, 0.toString) ~>
          Header(HEADER_TRUTH_VERSION, 0.toString) ~>
          Content(JsonContentType, "[]")
      case DatasetDropper.FailureNotFound =>
        notFoundError(datasetId)
      case DatasetDropper.FailureWriteLock =>
        writeLockError(datasetId)
    }
  }

  private def preconditionFailed = datasetErrorResponse(PreconditionFailed, RequestError.PRECONDITION_FAILED)

  private def doExportFile(req: HttpRequest): HttpResponse = {
    val servReq = req.servletRequest
    val precondition = req.precondition
    val schemaHash = Option(servReq.getParameter("schemaHash"))
    val ifModifiedSince = req.dateTimeHeader("If-Modified-Since")
    val onlyColumns = Option(servReq.getParameterValues("c")).map(_.flatMap { c =>
      norm(c).split(',').map(new UserColumnId(_)) }).map(UserColumnIdSet(_ : _*))
    val limit = Option(servReq.getParameter("limit")).map { limStr =>
      try {
        limStr.toLong
      } catch {
        case _: NumberFormatException => return contentTypeBadRequest("Bad limit")
      }
    }
    val offset = Option(servReq.getParameter("offset")).map { offStr =>
      try {
        offStr.toLong
      } catch {
        case _: NumberFormatException => return contentTypeBadRequest("Bad offset")
      }
    }
    val copy = Option(servReq.getParameter("copy")).getOrElse(COPY_LATEST).toLowerCase match {
      case COPY_LATEST => LatestCopy
      case COPY_PUBLISHED => PublishedCopy
      case COPY_WORKING => WorkingCopy
      case other =>
        try {
          Snapshot(other.toLong)
        } catch {
          case _: NumberFormatException => return contentTypeBadRequest("Bad copy selector")
        }
    }
    val sorted = Option(servReq.getParameter("sorted")).getOrElse("true").toLowerCase match {
      case "true" => true
      case "false" =>
        if(limit.isDefined || offset.isDefined) {
          return contentTypeBadRequest("Cannot page through an unsorted export")
        }
        false
      case _ => return contentTypeBadRequest("Bad sorted selector")
    }
    val rowId = Option(servReq.getParameter("row_id"))
    val suffix = locally {
      val md = MessageDigest.getInstance(suffixHashAlg)
      md.update(formatDatasetId(datasetId).getBytes(UTF_8))
      md.update(schemaHash.toString.getBytes(UTF_8))
      md.update(onlyColumns.toString.getBytes(UTF_8))
      md.update(limit.toString.getBytes(UTF_8))
      md.update(offset.toString.getBytes(UTF_8))
      md.update(copy.toString.getBytes)
      md.update(sorted.toString.getBytes)
      md.digest()
    }
    precondition.filter(_.endsWith(suffix)) match {
      case Right(newPrecond) =>
        val upstreamPrecondition = newPrecond.map(_.dropRight(suffix.length))
        resp => {
          val found = datasetContents(datasetId, schemaHash, copy, onlyColumns, limit, offset,
            upstreamPrecondition, ifModifiedSince, sorted, rowId) {
            case Left(newSchema) =>
              mismatchedSchema(ExportRequestError.MISMATCHED_SCHEMA, datasetId, newSchema)(resp)
            case Right((etag, schema, rowIdCol, locale, approxRowCount, rows)) =>
              resp.setContentType("application/json")
              resp.setCharacterEncoding("utf-8")
              ETag(etag.append(suffix))(resp)
              val out = new BufferedWriter(resp.getWriter)
              val jsonWriter = new CompactJsonWriter(out)
              out.write("[{\"approximate_row_count\":")
              out.write(JNumber(approxRowCount).toString)
              out.write("\n ,\"locale\":")
              jsonWriter.write(JString(locale))
              rowIdCol.foreach { rid =>
                out.write("\n ,\"pk\":")
                jsonWriter.write(JsonEncode.toJValue(rid))
              }
              out.write("\n ,\"schema\":")
              jsonWriter.write(JsonEncode.toJValue(schema))
              out.write("\n }")
              while(rows.hasNext) {
                out.write("\n,")
                jsonWriter.write(JArray(rows.next()))
              }
              out.write("\n]\n")
              out.flush()
          }

          found match {
            case Exporter.Success(_) => // ok good
            case Exporter.NotFound => notFoundError(datasetId)(resp)
            case Exporter.NotModified(etags) => notModified(etags.map(_.append(suffix)))(resp)
            case Exporter.PreconditionFailedBecauseNoMatch => preconditionFailed(resp)
            case Exporter.InvalidRowId => datasetBadRequest(ExportRequestError.INVALID_ROW_ID)(resp)
          }
        }
      case Left(Precondition.FailedBecauseNoMatch) =>
        preconditionFailed
    }
  }
}

object DatasetResource{
  type datasetContentsFunc = Either[Schema, (EntityTag, Seq[SchemaField], Option[UserColumnId], String, Long,
    Iterator[Array[JValue]])] => Unit
}
