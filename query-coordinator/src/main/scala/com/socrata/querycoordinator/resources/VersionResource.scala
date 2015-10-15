package com.socrata.querycoordinator.resources

import com.rojoma.json.v3.ast.{JString, JObject}
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.querycoordinator.BuildInfo
import com.socrata.http.server.implicits._


class VersionResource extends QCResource {

  val version = JsonUtil.renderJson(JObject(BuildInfo.toMap.mapValues(v => JString(v.toString))))

  override val get: HttpService = {
    _: HttpRequest => OK ~> Content("application/json", version)
  }

}
object VersionResource {
  def apply(): VersionResource = {
    new VersionResource()
  }
}
