package com.socrata.datacoordinator.mover

import java.net.URL

import com.rojoma.json.v3.util.AutomaticJsonDecodeBuilder
import com.socrata.http.client.{HttpClient, RequestBuilder}

import com.socrata.datacoordinator.id.DatasetInternalName

class ArchivalSecondaryClient(httpClient: HttpClient, url: URL) {
  private case class ArchivalVersion(name: String)
  private implicit val versionDecode = AutomaticJsonDecodeBuilder[ArchivalVersion]

  def check(): Unit = {
    for(resp <- httpClient.execute(RequestBuilder(url).p("version").get)) {
      if(resp.resultCode != 200) {
        throw new Exception("Failed to check archival secondary version")
      }
      resp.value[ArchivalVersion]() match {
        case Right(version) if version.name == "archivalCommon" =>
          // ok
        case _ =>
          throw new Exception("Archival URL does not appear to name an archival secondary!")
      }
    }
  }

  def addName(from: DatasetInternalName, to: DatasetInternalName): Unit = {
    for(resp <- httpClient.execute(RequestBuilder(url).p("archival", "alias", from.underlying, to.underlying).method("PUT").form(Nil))) {
      if(resp.resultCode != 200) {
        throw new Exception(s"Non-200 result from adding the name ${to} to ${from} in the archival secondary!")
      }
    }
  }

  def removeName(from: DatasetInternalName, exName: DatasetInternalName): Unit = {
    for(resp <- httpClient.execute(RequestBuilder(url).p("archival", "alias", from.underlying, exName.underlying).method("DELETE").form(Nil))) {
      if(resp.resultCode != 200) {
        throw new Exception(s"Non-200 result from removing name ${from} frp, the archival secondary!")
      }
    }
  }
}
