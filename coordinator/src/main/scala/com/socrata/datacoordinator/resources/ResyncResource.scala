package com.socrata.datacoordinator.resources

import java.util.UUID
import com.socrata.datacoordinator.id.DatasetId

import com.socrata.http.server.{HttpRequest, HttpResponse}
import com.socrata.http.server.responses._


case class ResyncResource(
  performResync: (String, DatasetId) => Unit
)(resource: DatasetId, secondary: String) extends SodaResource {
  override def put = req => {
    performResync(secondary, resource)
    OK
  }
}
