package com.socrata.querycoordinator

import javax.servlet.http.HttpServletRequest
import com.socrata.http.server.HttpResponse
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._

class Service extends (HttpServletRequest => HttpResponse) {
  def apply(req: HttpServletRequest) =
    OK ~> Content("Hello world!\n")
}
