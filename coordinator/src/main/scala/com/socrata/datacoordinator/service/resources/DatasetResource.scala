package com.socrata.datacoordinator.service.resources

import com.socrata.datacoordinator.id.{UserColumnId, DatasetId}
import com.rojoma.json.ast._
import com.rojoma.json.codec.JsonCodec
import com.socrata.http.server.responses._
import org.joda.time.format.ISODateTimeFormat
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import org.joda.time.DateTime
import com.rojoma.json.util.JsonArrayIterator
import com.rojoma.json.io._
import com.socrata.http.server.HttpResponse
import javax.activation.{MimeTypeParseException, MimeType}
import java.io.{OutputStreamWriter, BufferedWriter, BufferedOutputStream, UnsupportedEncodingException}
import com.ibm.icu.text.Normalizer
import com.socrata.http.common.util.{TooMuchDataWithoutAcknowledgement, AcknowledgeableReader}
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.service.mutator._
import com.socrata.http.server.implicits._

import DatasetResource._
import java.security.MessageDigest
import com.socrata.datacoordinator.util.collection.UserColumnIdSet
import com.socrata.datacoordinator.truth._
import com.socrata.datacoordinator.truth.metadata.SchemaField
import scala.util.Try
import java.nio.charset.StandardCharsets
import com.socrata.http.server.util.{EntityTag, Precondition}
import com.socrata.datacoordinator.service.Exporter
import com.rojoma.json.io.IdentifierEvent
import com.rojoma.json.ast.JString
import com.socrata.datacoordinator.truth.metadata.Schema
import com.socrata.datacoordinator.service.resources.DatasetResource.DatasetContentOptions
import com.socrata.datacoordinator.truth.Snapshot
import com.rojoma.json.io.FieldEvent
import com.rojoma.json.io.StringEvent
import com.socrata.datacoordinator.util.IndexedTempFile
import com.rojoma.simplearm.util._
import com.rojoma.simplearm.Managed

class DatasetResource(upserter: UniversalUpserter,
                      deleteDataset: DatasetId => DatasetDropper.Result,
                      datasetContents: DatasetContentOptions => DatasetContentProcessor => Exporter.Result[Unit],
                      formatDatasetId: DatasetId => String,
                      commandReadLimit: Long,
                      indexedTempFileProvider: () => Managed[IndexedTempFile]) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[DatasetResource])
  val errors = new Errors(formatDatasetId)

  val dateTimeFormat = ISODateTimeFormat.dateTime

  def withUpsertScriptResponse[T](f: => HttpResponse): HttpResponse = {
    try {
      f
    } catch {
      case e: TooMuchDataWithoutAcknowledgement =>
        errors.upsertRowTooLarge(e.limit)
      case r: JsonReaderException =>
        errors.malformedJson(r)
      case e: MutationException =>
        errors.mutationException(e)
    }
  }

  def doUpsert(datasetId: DatasetId)(req: HttpServletRequest)(resp: HttpServletResponse) {
    for(indexedTempFile <- indexedTempFileProvider()) {
      val builtResponse = withUpsertScriptResponse {
        jsonStream(req, commandReadLimit) match {
          case Right((events, boundResetter)) =>
            val iteratorOrError = try {
              Right(JsonArrayIterator[JValue](events))
            } catch { case _: JsonBadParse =>
              Left(errors.contentNotJsonArray)
            }
            iteratorOrError match {
              case Right(iterator) =>
                val (version, lastModified) = upserter.upsertScript(datasetId, iterator.map { ev => boundResetter(); ev }, indexedTempFile)
                OK ~>
                  DataCoordinatorResource.ApplicationJson ~>
                  Header("X-SODA2-Truth-Last-Modified", dateTimeFormat.print(lastModified)) ~>
                  Header("X-SODA2-Truth-Version", version.toString) ~>
                  Stream { w =>
                    val bw = new BufferedOutputStream(w)
                    bw.write('[')
                    (0L until indexedTempFile.recordCount).foreach(new Function1[Long, Unit] {
                      var didOne = false
                      def apply(i: Long) {
                        if(didOne) bw.write(',')
                        else didOne = true
                        val record = indexedTempFile.readRecord(i).getOrElse { sys.error("Job " + i + " didn't cause a record in the temp file?") }
                        record.writeTo(bw)
                      }
                    })
                    bw.write(']')
                    bw.flush()
                  }
              case Left(error) =>
                error
            }
          case Left(response) =>
            response
        }
      }
      builtResponse(resp)
    }
  }

  def doDeleteDataset(datasetId: DatasetId)(req: HttpServletRequest): HttpResponse = {
    deleteDataset(datasetId) match {
      case DatasetDropper.Success =>
        OK ~>
          Header("X-SODA2-Truth-Last-Modified", dateTimeFormat.print(DateTime.now)) ~>
          Header("X-SODA2-Truth-Version", 0.toString) ~>
          ContentType("application/json; charset=utf-8") ~> Content("[]")
      case DatasetDropper.FailureNotFound =>
        errors.notFoundError(formatDatasetId(datasetId))
      case DatasetDropper.FailureWriteLock =>
        errors.writeLockError(datasetId)
    }
  }

  val suffixHashAlg = "SHA1"
  val suffixHashLen = MessageDigest.getInstance(suffixHashAlg).getDigestLength
  def doExportFile(datasetId: DatasetId)(req: HttpServletRequest): HttpResponse = {
    val precondition = req.precondition
    val ifModifiedSince = req.dateTimeHeader("if-modified-since")
    val schemaHash = Option(req.getParameter("schemaHash"))
    val onlyColumns = Option(req.getParameterValues("c")).map(_.flatMap { c => norm(c).split(',').map(new UserColumnId(_)) }).map(UserColumnIdSet(_ : _*))
    val limit = Option(req.getParameter("limit")).map { limStr =>
      try {
        limStr.toLong
      } catch {
        case _: NumberFormatException =>
          return errors.badLimit(limStr)
      }
    }
    val offset = Option(req.getParameter("offset")).map { offStr =>
      try {
        offStr.toLong
      } catch {
        case _: NumberFormatException =>
          return errors.badOffset(offStr)
      }
    }
    val copy = Option(req.getParameter("copy")) match {
      case Some(copyRaw) =>
        copySelectorFromString(copyRaw).getOrElse {
          return errors.badCopySelector(copyRaw)
        }
      case None =>
        LatestCopy
    }
    val sorted = Option(req.getParameter("sorted")) match {
      case Some(sortedRaw) =>
        sortedRaw.toLowerCase match {
          case "true" =>
            true
          case "false" =>
            if(limit.isDefined || offset.isDefined) return errors.noUnsortedPaging
            false
          case _ =>
            return errors.badSortedSelector(sortedRaw)
        }
      case None =>
        true
    }
    val suffix = locally {
      val md = MessageDigest.getInstance(suffixHashAlg)
      md.update(formatDatasetId(datasetId).getBytes(StandardCharsets.UTF_8))
      md.update(schemaHash.toString.getBytes(StandardCharsets.UTF_8))
      md.update(onlyColumns.toString.getBytes(StandardCharsets.UTF_8))
      md.update(limit.toString.getBytes(StandardCharsets.UTF_8))
      md.update(offset.toString.getBytes(StandardCharsets.UTF_8))
      md.update(copy.toString.getBytes(StandardCharsets.UTF_8))
      md.update(sorted.toString.getBytes(StandardCharsets.UTF_8))
      md.digest()
    }
    precondition.filter(_.endsWith(suffix)) match {
      case Right(newPrecond) =>
        def responder(resp: HttpServletResponse) {
          val upstreamPrecondition = newPrecond.map(_.dropRight(suffix.length))
          val found = datasetContents(DatasetContentOptions(datasetId = datasetId,
                                                            schemaHash = schemaHash,
                                                            copy = copy,
                                                            columns = onlyColumns,
                                                            limit = limit,
                                                            offset = offset,
                                                            precondition = upstreamPrecondition,
                                                            ifModifiedSince = ifModifiedSince,
                                                            sorted = sorted)) {
            case Left(newSchema) =>
              errors.mismatchedSchema("req.export.mismatched-schema", datasetId, newSchema)(resp)
              return
            case Right((etag, schema, rowIdCol, locale, approxRowCount, rows)) =>
              resp.setContentType("application/json; charset=utf-8")
              ETag(etag.append(suffix))(resp)
              val out = new BufferedWriter(new OutputStreamWriter(resp.getOutputStream, StandardCharsets.UTF_8))
              val jsonWriter = new CompactJsonWriter(out)
              out.write("[{\"approximate_row_count\":")
              jsonWriter.write(JNumber(approxRowCount))
              out.write("\n ,\"locale\":")
              jsonWriter.write(JString(locale))
              rowIdCol.foreach { rid =>
                out.write("\n ,\"pk\":")
                jsonWriter.write(JsonCodec.toJValue(rid))
              }
              out.write("\n ,\"schema\":")
              jsonWriter.write(JsonCodec.toJValue(schema))
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
            case Exporter.NotFound =>
              errors.notFoundError(formatDatasetId(datasetId))
            case Exporter.NotModified(etags) =>
              errors.notModified(etags)(resp)
            case Exporter.PreconditionFailedBecauseNoMatch =>
              errors.preconditionFailed(resp)
          }
        }

        responder
      case Left(Precondition.FailedBecauseNoMatch) =>
        errors.preconditionFailed
    }
  }

  case class service(datasetId: DatasetId) extends DataCoordinatorResource {
    override def post = doUpsert(datasetId)
    override def delete = doDeleteDataset(datasetId)
    override def get = doExportFile(datasetId)
  }
}

object DatasetResource {
  case class DatasetContentOptions(datasetId: DatasetId,
                                   schemaHash: Option[String],
                                   copy: CopySelector,
                                   columns: Option[UserColumnIdSet],
                                   limit: Option[Long],
                                   offset: Option[Long],
                                   precondition: Precondition,
                                   ifModifiedSince: Option[DateTime],
                                   sorted: Boolean)

  type DatasetContentProcessor = Either[Schema, (EntityTag, Seq[SchemaField], Option[UserColumnId], String, Long, Iterator[Array[JValue]])] => Unit

  def copySelectorFromString(s: String): Option[CopySelector] =
    s.toLowerCase match {
      case "latest" => Some(LatestCopy)
      case "published" => Some(PublishedCopy)
      case "working" => Some(WorkingCopy)
      case other => Try(Snapshot(other.toInt)).toOption
    }

  val normalizationMode: Normalizer.Mode = Normalizer.NFC

  def norm(s: String) = Normalizer.normalize(s, normalizationMode)

  def normalizeJson(token: JsonEvent): JsonEvent = {
    def position(t: JsonEvent) = { t.position = token.position; t }
    token match {
      case StringEvent(s) =>
        position(StringEvent(norm(s)))
      case FieldEvent(s) =>
        position(FieldEvent(norm(s)))
      case IdentifierEvent(s) =>
        position(IdentifierEvent(norm(s)))
      case other =>
        other
    }
  }

  def jsonStream(req: HttpServletRequest, approximateMaxDatumBound: Long): Either[HttpResponse, (Iterator[JsonEvent], () => Unit)] = {
    val nullableContentType = req.getContentType
    if(nullableContentType == null)
      return Left(Errors.noContentType)
    val contentType =
      try { new MimeType(nullableContentType) }
      catch { case _: MimeTypeParseException =>
        return Left(Errors.contentTypeUnparsable(nullableContentType))
      }
    if(!contentType.`match`("application/json")) {
      return Left(Errors.contentNotJson(contentType))
    }
    val reader =
      try { req.getReader }
      catch { case _: UnsupportedEncodingException =>
        return Left(Errors.contentUnknownCharset(contentType))
      }
    val boundedReader = new AcknowledgeableReader(reader, approximateMaxDatumBound)
    Right((new FusedBlockJsonEventIterator(boundedReader).map(normalizeJson), boundedReader.acknowledge _))
  }
}
