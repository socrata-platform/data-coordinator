package com.socrata.datacoordinator.resources

import java.util.UUID
import com.socrata.datacoordinator.id.DatasetId

import com.socrata.http.server.{HttpRequest, HttpResponse}
import com.socrata.http.server.responses._

case class ResyncResource(
  formatDatasetId: DatasetId => String,
  ensureInSecondary: (String, DatasetId) => Boolean,
  performResync: (String, DatasetId) => Unit
)(resource: DatasetId, secondary: String) extends ErrorHandlingSodaResource(formatDatasetId) {
  override def put = req => {
    ensureInSecondary(secondary, resource) match {
      case true =>
        performResync(secondary, resource)
        OK
      case false =>
        datasetNotInSecondary(resource, secondary)
    }

  }
}
