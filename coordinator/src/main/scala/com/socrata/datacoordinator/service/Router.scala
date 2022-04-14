package com.socrata.datacoordinator.service

import com.socrata.datacoordinator.id.{DatasetId, RollupName}
import com.socrata.datacoordinator.resources.SodaResource
import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.{Extractor, SimpleRouteContext}
import com.socrata.http.server.routing.SimpleRouteContext._
import com.socrata.datacoordinator.common.util.DatasetIdNormalizer._

case class Router(parseDatasetId: String => Option[DatasetId],
                  notFoundDatasetResource: Option[String] => SodaResource,
                  datasetResource: DatasetId => SodaResource,
                  datasetSchemaResource: DatasetId => SodaResource,
                  datasetSnapshotsResource: DatasetId => SodaResource,
                  datasetSnapshotResource: (DatasetId, Long) => SodaResource,
                  datasetLogResource: (DatasetId, Long) => SodaResource,
                  datasetRollupResource: DatasetId => SodaResource,
                  snapshottedResource: SodaResource,
                  secondaryManifestsResource: Option[String] => SodaResource,
                  secondaryManifestsCollocateResource: String => SodaResource,
                  secondaryManifestsMetricsResource: (String, Option[DatasetId]) => SodaResource,
                  secondaryManifestsMoveResource: (Option[String], DatasetId) => SodaResource,
                  secondaryManifestsMoveJobResource: (String, String) => SodaResource,
                  secondaryMoveJobsJobResource: String => SodaResource,
                  datasetSecondaryStatusResource: (Option[String], DatasetId) => SodaResource,
                  secondariesOfDatasetResource: DatasetId => SodaResource,
                  collocationManifestsResource: (Option[String], Option[String]) => SodaResource,
                  resyncResource: (DatasetId, String) => SodaResource,
                  versionResource: SodaResource) {

  type OptString = Option[String]




  implicit object DatasetIdExtractor extends Extractor[DatasetId] {
    def extract(s: String): Option[DatasetId] = parseDatasetId(norm(s))
  }

  implicit object RollupNameExtractor extends Extractor[RollupName] {
    def extract(s: String): Option[RollupName] = Some(new RollupName(s))
  }

  implicit object StringExtractor extends Extractor[OptString]{
    def extract(s: String): Option[OptString] = if (s.isEmpty) Some(None) else Some(Some(s))
  }



  val routes = locally {
    import SimpleRouteContext._
    Routes(
      // "If the thing is parsable as a DatasetId, do something with it, otherwise give a
      // SODA2 not-found response"
      Route("/dataset", notFoundDatasetResource(None)),
      Route("/dataset/{OptString}", notFoundDatasetResource),
      Route("/dataset/{DatasetId}", datasetResource),
      Route("/dataset/{DatasetId}/schema", datasetSchemaResource),
      Route("/dataset/{DatasetId}/snapshots", datasetSnapshotsResource),
      Route("/dataset/{DatasetId}/snapshots/{Long}", datasetSnapshotResource),
      Route("/dataset/{DatasetId}/log/{Long}", datasetLogResource),

      Route("/dataset-rollup/{DatasetId}", datasetRollupResource),
      Route("/snapshotted", snapshottedResource),

      Route("/secondary-manifest", secondaryManifestsResource(None)),
      Route("/secondary-manifest/{OptString}", secondaryManifestsResource),
      Route("/secondary-manifest/{OptString}/{DatasetId}", datasetSecondaryStatusResource),
      Route("/secondary-manifest/{String}/collocate", secondaryManifestsCollocateResource),

      Route("/secondary-manifest/metrics/{String}", secondaryManifestsMetricsResource(_: String, None)),
      Route("/secondary-manifest/metrics/{String}/{DatasetId}",
        { (storeId: String, datasetId: DatasetId) => secondaryManifestsMetricsResource(storeId, Some(datasetId)) }),

      Route("/secondary-manifest/move/{DatasetId}", secondaryManifestsMoveResource(None, _: DatasetId)),
      Route("/secondary-manifest/move/{OptString}/{DatasetId}", secondaryManifestsMoveResource),

      Route("/secondary-manifest/move/{String}/job/{String}", secondaryManifestsMoveJobResource),

      Route("/secondary-move-jobs/job/{String}", secondaryMoveJobsJobResource),

      Route("/secondaries-of-dataset/{DatasetId}", secondariesOfDatasetResource),

      Route("/collocation-manifest", collocationManifestsResource(None, None)),
      Route("/collocation-manifest/{OptString}", collocationManifestsResource(_: Option[String], None)),
      Route("/collocation-manifest/{OptString}/{OptString}", collocationManifestsResource),

      Route("/resync/{DatasetId}/{String}", resyncResource),

      Route("/version", versionResource)
    )
  }


  def handler(req: HttpRequest): HttpResponse =
    routes(req.requestPath) match {
      case Some(s) => s(req)
      case None => NotFound
    }

}
