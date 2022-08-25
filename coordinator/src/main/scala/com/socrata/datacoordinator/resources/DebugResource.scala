package com.socrata.datacoordinator.resources

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets

import com.socrata.datacoordinator.service.ServiceUtil._
import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.rojoma.json.v3.util.{AutomaticJsonDecodeBuilder, JsonUtil, SimpleHierarchyDecodeBuilder, InternalTag}
import com.rojoma.json.v3.io.JsonReaderException
import com.socrata.datacoordinator.service.{BoundedInputStream, ReaderExceededBound}
import com.socrata.datacoordinator.util.DebugState

object DebugResource extends SodaResource {
  sealed abstract class DebugCommand

  case class AnalyzeUpsert(threadId: Long) extends DebugCommand
  object AnalyzeUpsert {
    implicit val jDecode = AutomaticJsonDecodeBuilder[AnalyzeUpsert]
  }

  implicit val jCodec = SimpleHierarchyDecodeBuilder[DebugCommand](InternalTag("command")).
    branch[AnalyzeUpsert]("analyze-upsert").
    build

  private def doPost(req: HttpRequest): HttpResponse = {
    val command =
      try {
        JsonUtil.readJson[DebugCommand](new InputStreamReader(new BoundedInputStream(req.inputStream, 10240), StandardCharsets.UTF_8)) match {
          case Right(cmd) => cmd
          case Left(e) => return BadRequest ~> Content("text/plain", "Malformed debug command: " + e.english)
        }
      } catch {
        case e: ReaderExceededBound =>
          return RequestEntityTooLarge ~> Content("text/plain", "Body too large")
        case e: JsonReaderException =>
          return BadRequest ~> Content("text/plain", "Invalid json")
      }

    command match {
      case AnalyzeUpsert(threadId) =>
        DebugState.requestUpsertExplanation(threadId)
        OK ~> Content("text/plain", "Request made")
    }
  }

  override val post: HttpService = doPost _
}
