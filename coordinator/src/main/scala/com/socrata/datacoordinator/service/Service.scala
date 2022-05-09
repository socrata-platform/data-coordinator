package com.socrata.datacoordinator.service

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.io._
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.resources._
import com.socrata.datacoordinator.truth.loader._
import com.socrata.http.server.responses._
import com.socrata.http.server.util.handlers.{LoggingOptions, NewLoggingHandler, ThreadRenamingHandler}
import com.socrata.http.server.util.ErrorAdapter
import com.socrata.http.server.util.RequestId.ReqIdHeader
import com.socrata.thirdparty.metrics.{MetricsReporter, SocrataHttpSupport}
import java.util.concurrent.atomic.AtomicInteger

import com.socrata.datacoordinator.service.ServiceUtil._
import com.socrata.http.server._
import com.socrata.http.server.implicits._
import com.socrata.datacoordinator.external._
import Service._
import com.socrata.datacoordinator.common.collocation.CollocationLock
import com.socrata.datacoordinator.resources.collocation._
import org.slf4j.LoggerFactory

/**
 * The main HTTP REST resource servicing class for the data coordinator.
 * It should probably be broken up into multiple classes per resource.
 */
class Service(serviceConfig: ServiceConfig,
              formatDatasetId: DatasetId => String,
              parseDatasetId: String => Option[DatasetId],
              notFoundDatasetResource: (Option[String], (=> HttpResponse) => HttpResponse) => NotFoundDatasetResource,
              datasetResource: (DatasetId, (=> HttpResponse) => HttpResponse) => DatasetResource,
              datasetSchemaResource: DatasetId => DatasetSchemaResource,
              datasetSnapshotsResource: DatasetId => DatasetSnapshotsResource,
              datasetSnapshotResource: (DatasetId, Long) => DatasetSnapshotResource,
              datasetLogResource: (DatasetId, Long) => SodaResource,
              datasetRollupResource: DatasetId => DatasetRollupResource,
              snapshottedResource: SodaResource,
              secondaryManifestsResource: Option[String] => SecondaryManifestsResource,
              secondaryManifestsCollocateResource: String => SecondaryManifestsCollocateResource,
              secondaryManifestsMetricsResource: (String, Option[DatasetId]) => SecondaryManifestsMetricsResource,
              secondaryManifestsMoveResource: (Option[String], DatasetId) => SecondaryManifestsMoveResource,
              secondaryManifestsMoveJobResource: (String, String) => SecondaryManifestsMoveJobResource,
              secondaryMoveJobsJobResource: String => SecondaryMoveJobsJobResource,
              datasetSecondaryStatusResource: (Option[String], DatasetId) => DatasetSecondaryStatusResource,
              collocationManifestsResource: (Option[String], Option[String]) => CollocationManifestsResource,
              resyncResource: (DatasetId, String) => ResyncResource,
              secondariesOfDatasetResource: DatasetId => SecondariesOfDatasetResource
             ) extends CoordinatorErrorsAndMetrics(formatDatasetId)
{
  override val log = org.slf4j.LoggerFactory.getLogger(classOf[Service])


  // Repeatedly tries to get a new thread by bumping numThreads, using
  // compareAndSet to make sure our value is valid
  @annotation.tailrec
  private def tryGetMutationThread(timeoutMs: Long, waitInterval: Int = 100): Boolean = {
    if (timeoutMs <= 0) {
      log.info("tryGetMutationThread: timed out")
      false
    } else {
      val currThreads = numThreads.get()
      if (currThreads >= serviceConfig.maxMutationThreads ||
          !numThreads.compareAndSet(currThreads, currThreads + 1)) {
        if (currThreads >= serviceConfig.maxMutationThreads)
          log.info(s"tryGetMutationThread: too many threads ($currThreads), waiting for some to finish")
        Thread sleep waitInterval
        tryGetMutationThread(timeoutMs - waitInterval, waitInterval)
      } else {
        true
      }
    }
  }

  def withMutationScriptResults[T](f: => HttpResponse): HttpResponse = {
    if (!tryGetMutationThread(serviceConfig.mutationResourceTimeout.toMillis)) {
      datasetErrorResponse(ServiceUnavailable, ThreadsMutationError.MAXED_OUT)
    } else {
      try {
        f
      } catch {
        case e: ReaderExceededBound =>
          datasetErrorResponse(RequestEntityTooLarge, BodyRequestError.COMMAND_TOO_LARGE,
            "bytes-without-full-datum" -> JNumber(e.bytesRead))
        case r: JsonReaderException =>
          datasetBadRequest(BodyRequestError.MALFORMED_JSON,
            "row" -> JNumber(r.row),
            "column" -> JNumber(r.column))
        case em: Mutator.RowDataException =>  // subclass of MutationException; these have a command subindex to indicate progress
          em match {
            case Mutator.InvalidValue(datasetName, userColumnId, typeName, value) =>
              datasetBadRequest(RowUpdateError.UNPARSABLE_VALUE,
                "commandIndex" -> JNumber(em.index),
                "commandSubIndex" -> JNumber(em.subindex),
                "dataset" -> JString(formatDatasetId(datasetName)),
                "column" -> JsonEncode.toJValue(userColumnId),
                "type" -> JString(typeName.name),
                "value" -> value)
            case Mutator.InvalidUpsertCommand(datasetName, value) =>
              datasetBadRequest(ScriptRowDataUpdateError.INVALID_VALUE,
                "commandIndex" -> JNumber(em.index),
                "commandSubIndex" -> JNumber(em.subindex),
                "dataset" -> JString(formatDatasetId(datasetName)),
                "value" -> value)
            case Mutator.UnknownColumnId(datasetName, cid) =>
              datasetBadRequest(RowUpdateError.UNKNOWN_COLUMN,
                "commandIndex" -> JNumber(em.index),
                "commandSubIndex" -> JNumber(em.subindex),
                "dataset" -> JString(formatDatasetId(datasetName)),
                "column" -> JsonEncode.toJValue(cid))
            case _ => // if we're not handling something, don't just eat it
              datasetBadRequest(RequestError.UNHANDLED_ERROR,
                "error" -> JString(em.getMessage))
          }
        case em: Mutator.MutationException =>
          em match {
            case Mutator.EmptyCommandStream() =>
              datasetBadRequest(ScriptHeaderRequestError.MISSING,
              "commandIndex" -> JNumber(em.index))
            case Mutator.CommandIsNotAnObject(value) =>
              datasetBadRequest(ScriptCommandRequestError.NON_OBJECT,
                "commandIndex" -> JNumber(em.index),
                "value" -> value)
            case Mutator.MissingCommandField(obj, field) =>
              datasetBadRequest(ScriptCommandRequestError.MISSING_FIELD,
                "commandIndex" -> JNumber(em.index),
                "object" -> obj,
                "field" -> JString(field))
            case Mutator.MismatchedSchemaHash(name, schema) =>
              mismatchedSchema(ScriptHeaderRequestError.MISMATCHED_SCHEMA, name, schema,
                "commandIndex" -> JNumber(em.index))
            case Mutator.MismatchedDataVersion(name, version) =>
              mismatchedDataVersion(ScriptHeaderRequestError.MISMATCHED_DATA_VERSION, name, version,
                "commandIndex" -> JNumber(em.index))
            case Mutator.NoSuchColumnLabel(name, label) =>
              noSuchColumnLabel(ScriptCommandRequestError.UNKNOWN_LABEL, name,
                "commandIndex" -> JNumber(em.index),
                "label" -> JString(label))
            case Mutator.InvalidCommandFieldValue(obj, field, value) =>
              datasetBadRequest(ScriptCommandRequestError.INVALID_FIELD,
                "commandIndex" -> JNumber(em.index),
                "object" -> obj,
                "field" -> JString(field),
                "value" -> value)
            case Mutator.NoSuchDataset(name) =>
              notFoundError(name, "commandIndex" -> JNumber(em.index))
            case Mutator.SecondaryStoresNotUpToDate(name, stores) =>
              datasetErrorResponse(Conflict, DatasetUpdateError.SECONDARIES_OUT_OF_DATE,
                "commandIndex" -> JNumber(em.index),
                "dataset" -> JString(formatDatasetId(name)),
                "stores" -> JsonEncode.toJValue(stores))
            case Mutator.NoSuchRollup(name) =>
              datasetErrorResponse(NotFound, RollupDeleteError.DOES_NOT_EXIST,
                "commandIndex" -> JNumber(em.index),
                "rollup" -> JString(name.underlying))
            case Mutator.InvalidRollup(name) =>
              datasetErrorResponse(BadRequest, RollupCreateError.INVALID,
                "commandIndex" -> JNumber(em.index),
                "rollup" -> JString(name.underlying))
            case Mutator.NoSuchIndex(name) =>
              datasetErrorResponse(NotFound, IndexDeleteError.DOES_NOT_EXIST,
                "commandIndex" -> JNumber(em.index),
                "index" -> JString(name.underlying))
            case Mutator.InvalidIndex(name) =>
              datasetErrorResponse(BadRequest, IndexCreateError.INVALID,
                "commandIndex" -> JNumber(em.index),
                "index" -> JString(name.underlying))
            case Mutator.CannotAcquireDatasetWriteLock(name) =>
              writeLockError(name, "commandIndex" -> JNumber(em.index))
            case Mutator.SystemInReadOnlyMode() =>
              datasetErrorResponse(ServiceUnavailable, UpdateError.READ_ONLY_MODE,
                "commandIndex" -> JNumber(em.index))
            case Mutator.InvalidLocale(locale) =>
              datasetBadRequest(DatasetCreateError.INVALID_LOCALE,
                "commandIndex" -> JNumber(em.index),
                "locale" -> JString(locale))
            case Mutator.IncorrectLifecycleStage(name, currentStage, expectedStage) =>
              datasetErrorResponse(Conflict, DatasetUpdateError.INVALID_STATE,
                "commandIndex" -> JNumber(em.index),
                "dataset" -> JString(formatDatasetId(name)),
                "actual-state" -> JString(currentStage.name),
                "expected-state" -> JArray(expectedStage.toSeq.map(_.name).map(JString)))
            case Mutator.InitialCopyDrop(name) =>
              datasetErrorResponse(Conflict, DatasetUpdateError.INITIAL_COPY_DROP,
                "commandIndex" -> JNumber(em.index),
                "dataset" -> JString(formatDatasetId(name)))
            case Mutator.OperationAfterDrop(name) =>
              datasetBadRequest(DatasetUpdateError.OPERATION_AFTER_DROP,
                "commandIndex" -> JNumber(em.index),
                "dataset" -> JString(formatDatasetId(name)))
            case Mutator.ColumnAlreadyExists(dataset, name) =>
              columnErrorResponse(ColumnUpdateError.EXISTS, em.index, dataset, name, Conflict)
            case Mutator.FieldNameAlreadyExists(dataset, name) =>
              fieldNameErrorResponse(ColumnUpdateError.EXISTS, em.index, dataset, name, Conflict)  // TODO : something different?
            case Mutator.IllegalColumnId(id) =>
              datasetBadRequest(ColumnUpdateError.ILLEGAL_ID,
                "commandIndex" -> JNumber(em.index),
                "id" -> JsonEncode.toJValue(id))
            case Mutator.NoSuchType(typeName) =>
              datasetBadRequest(UpdateError.TYPE_UNKNOWN,
                "commandIndex" -> JNumber(em.index),
                "type" -> JString(typeName.name))
            case Mutator.NoSuchColumn(dataset, name) =>
              columnErrorResponse(ColumnUpdateError.NOT_FOUND, em.index, dataset, name)
            case Mutator.InvalidSystemColumnOperation(dataset, name, _) =>
              columnErrorResponse(ColumnUpdateError.SYSTEM, em.index, dataset, name)
            case Mutator.PrimaryKeyAlreadyExists(datasetName, userColumnId, existingColumn) =>
              datasetBadRequest(RowIdentifierUpdateError.ALREADY_SET,
                "commandIndex" -> JNumber(em.index),
                "dataset" -> JString(formatDatasetId(datasetName)),
                "column" -> JsonEncode.toJValue(userColumnId),
                "existing-column" -> JsonEncode.toJValue(userColumnId))
            case Mutator.InvalidTypeForPrimaryKey(datasetName, userColumnId, typeName) =>
              datasetBadRequest(RowIdentifierUpdateError.INVALID_TYPE,
                "commandIndex" -> JNumber(em.index),
                "dataset" -> JString(formatDatasetId(datasetName)),
                "column" -> JsonEncode.toJValue(userColumnId),
                "type" -> JString(typeName.name))
            case Mutator.DuplicateValuesInColumn(dataset, name) =>
              columnErrorResponse(RowIdentifierUpdateError.DUPLICATE_VALUES, em.index, dataset, name)
            case Mutator.NullsInColumn(dataset, name) =>
              columnErrorResponse(RowIdentifierUpdateError.NULL_VALUES, em.index, dataset, name)
            case Mutator.NotPrimaryKey(dataset, name) =>
              columnErrorResponse(RowIdentifierUpdateError.NOT_ROW_IDENTIFIER, em.index, dataset, name)
            case Mutator.UpsertError(datasetName, NoPrimaryKey, _)=>
              datasetBadRequest(RowUpdateError.PRIMARY_KEY_NONEXISTENT_OR_NULL,
                "commandIndex" -> JNumber(em.index),
                "dataset" -> JString(formatDatasetId(datasetName)))
            case Mutator.UpsertError(datasetName, NoSuchRowToDelete(id), _) =>
              datasetBadRequest(RowUpdateError.NO_SUCH_ID,
                "commandIndex" -> JNumber(em.index),
                "dataset" -> JString(formatDatasetId(datasetName)),
                "value" -> id)
            case Mutator.UpsertError(datasetName, NoSuchRowToUpdate(id, _), _) =>
              datasetBadRequest(RowUpdateError.NO_SUCH_ID,
                "commandIndex" -> JNumber(em.index),
                "dataset" -> JString(formatDatasetId(datasetName)),
                "value" -> id)
            case Mutator.UpsertError(datasetName, VersionMismatch(id, expected, actual, _), rowVersionToJson) =>
              datasetBadRequest(UpdateError.ROW_VERSION_MISMATCH,
                "commandIndex" -> JNumber(em.index),
                "dataset" -> JString(formatDatasetId(datasetName)),
                "value" -> id,
                "expected" -> expected.map(rowVersionToJson).getOrElse(JNull),
                "actual" -> actual.map(rowVersionToJson).getOrElse(JNull))
            case Mutator.UpsertError(datasetName, VersionOnNewRow, _) =>
              datasetBadRequest(UpdateError.VERSION_ON_NEW_ROW,
                "commandIndex" -> JNumber(em.index),
                "dataset" -> JString(formatDatasetId(datasetName)))
            case Mutator.UpsertError(datasetName, InsertInUpdateOnly(id, _), _) =>
              datasetBadRequest(UpdateError.INSERT_IN_UPDATE_ONLY,
                "commandIndex" -> JNumber(em.index),
                "dataset" -> JString(formatDatasetId(datasetName)),
                "id" -> id)
            case _ => // if we're not handling something, don't just eat it
              datasetBadRequest(RequestError.UNHANDLED_ERROR,
                "error" -> JString(Option(em.getMessage).getOrElse(em.getClass.getName)))
          }
      } finally {
        // Decrease the thread count
        numThreads.decrementAndGet()
      }
    }
  }



  val router: Router = Router(parseDatasetId,
    notFoundDatasetResource = notFoundDatasetResource(_, withMutationScriptResults),
    datasetResource = datasetResource(_, withMutationScriptResults),
    datasetSchemaResource = datasetSchemaResource,
    datasetSnapshotsResource = datasetSnapshotsResource,
    datasetSnapshotResource = datasetSnapshotResource,
    datasetLogResource = datasetLogResource,
    datasetRollupResource = datasetRollupResource,
    snapshottedResource = snapshottedResource,
    secondaryManifestsResource = secondaryManifestsResource,
    secondaryManifestsCollocateResource = secondaryManifestsCollocateResource,
    secondaryManifestsMetricsResource = secondaryManifestsMetricsResource,
    secondaryManifestsMoveResource = secondaryManifestsMoveResource,
    secondaryManifestsMoveJobResource = secondaryManifestsMoveJobResource,
    secondaryMoveJobsJobResource = secondaryMoveJobsJobResource,
    collocationManifestsResource = collocationManifestsResource,
    datasetSecondaryStatusResource = datasetSecondaryStatusResource,
    secondariesOfDatasetResource = secondariesOfDatasetResource,
    resyncResource = resyncResource,
    versionResource = VersionResource)

  private val errorHandlingHandler = new ErrorAdapter(router.handler) {
    type Tag = String
    def onException(tag: Tag): HttpResponse = {
      val error = s"""{"errorCode":"internal","data":{"tag":"$tag"}}"""
      InternalServerError ~> Write(JsonContentType) { w => w.write(error)}
    }

    def errorEncountered(ex: Exception): Tag = {
      val uuid = java.util.UUID.randomUUID().toString
      log.error("Unhandled error; errorCode: internal; tag = " + uuid, ex)
      uuid
    }
  }

  private val logOptions = LoggingOptions(LoggerFactory.getLogger(""),
                                          logRequestHeaders = Set(ReqIdHeader, "X-Socrata-Resource"))

  private val metricsOptions = serviceConfig.metrics

  def run(port: Int, broker: ServerBroker) {
    for { reporter <- MetricsReporter.managed(metricsOptions) } {
      val server = new SocrataServerJetty(
                     ThreadRenamingHandler(
                       NewLoggingHandler(logOptions)(errorHandlingHandler)),
                     SocrataServerJetty.defaultOptions.
                       withPort(port).
                       withExtraHandlers(List(SocrataHttpSupport.getHandler(metricsOptions))).
                       withPoolOptions(SocrataServerJetty.Pool(serviceConfig.jettyThreadpool)).
                       withBroker(broker))
      server.run()
    }
  }
}

object Service {
  val numThreads = new AtomicInteger

  def apply(serviceConfig: ServiceConfig,
            formatDatasetId: DatasetId => String,
            parseDatasetId: String => Option[DatasetId],
            notFoundDatasetResource: (Option[String], (=> HttpResponse) => HttpResponse) => NotFoundDatasetResource,
            datasetResource: CollocationLock => (String => Option[(String, Int)]) => (DatasetId, (=> HttpResponse) => HttpResponse) => DatasetResource,
            datasetSchemaResource: DatasetId => DatasetSchemaResource,
            datasetSnapshotsResource: DatasetId => DatasetSnapshotsResource,
            datasetSnapshotResource: (DatasetId, Long) => DatasetSnapshotResource,
            datasetLogResource: (DatasetId, Long) => SodaResource,
            datasetRollupResource: DatasetId => DatasetRollupResource,
            snapshottedResource: SodaResource,
            secondaryManifestsResource: Option[String] => SecondaryManifestsResource,
            secondaryManifestsCollocateResource: CollocationLock => (String => Option[(String, Int)]) => String => SecondaryManifestsCollocateResource,
            secondaryManifestsMetricsResource: (String, Option[DatasetId]) => SecondaryManifestsMetricsResource,
            secondaryManifestsMoveResource: HostAndPort => (Option[String], DatasetId) => SecondaryManifestsMoveResource,
            secondaryManifestsMoveJobResource: HostAndPort => (String, String) => SecondaryManifestsMoveJobResource,
            secondaryMoveJobsJobResource: String => SecondaryMoveJobsJobResource,
            datasetSecondaryStatusResource: HostAndPort => (Option[String], DatasetId) => DatasetSecondaryStatusResource,
            collocationManifestsResource: CollocationLock => HostAndPort => (Option[String], Option[String]) => CollocationManifestsResource,
            resyncResource: (DatasetId, String) => ResyncResource,
            secondariesOfDatasetResource: DatasetId => SecondariesOfDatasetResource
           )(collocationLock: CollocationLock, hostAndPort: HostAndPort): Service = {
    new Service(
      serviceConfig,
      formatDatasetId,
      parseDatasetId,
      notFoundDatasetResource,
      datasetResource(collocationLock)(hostAndPort),
      datasetSchemaResource,
      datasetSnapshotsResource,
      datasetSnapshotResource,
      datasetLogResource,
      datasetRollupResource,
      snapshottedResource,
      secondaryManifestsResource,
      secondaryManifestsCollocateResource(collocationLock)(hostAndPort),
      secondaryManifestsMetricsResource,
      secondaryManifestsMoveResource(hostAndPort),
      secondaryManifestsMoveJobResource(hostAndPort),
      secondaryMoveJobsJobResource,
      datasetSecondaryStatusResource(hostAndPort),
      collocationManifestsResource(collocationLock)(hostAndPort),
      resyncResource,
      secondariesOfDatasetResource
    )
  }
}
