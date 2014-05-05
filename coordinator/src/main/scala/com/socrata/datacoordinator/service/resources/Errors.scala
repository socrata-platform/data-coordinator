package com.socrata.datacoordinator.service.resources

import com.socrata.datacoordinator.id.{UserColumnId, DatasetId}
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.rojoma.json.ast._
import com.socrata.http.server.util.EntityTag
import com.socrata.datacoordinator.service.mutator._
import com.rojoma.json.codec.JsonCodec
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.truth.loader.NoSuchRowToDelete
import com.rojoma.json.ast.JString
import com.socrata.datacoordinator.truth.loader.NoSuchRowToUpdate
import com.socrata.datacoordinator.truth.metadata.Schema
import com.socrata.datacoordinator.truth.loader.VersionMismatch
import javax.activation.MimeType
import com.socrata.datacoordinator.truth.loader.NoSuchRowToDelete
import com.rojoma.json.ast.JString
import com.socrata.datacoordinator.truth.loader.NoSuchRowToUpdate
import com.socrata.datacoordinator.truth.metadata.Schema
import com.socrata.datacoordinator.truth.loader.VersionMismatch
import com.socrata.http.server.HttpResponse

sealed class StaticErrors {
  import DataCoordinatorResource.err

  def notFoundError(datasetId: String) = // yes, string.  This is used when an invalid dataset ID is specified too.
    err(NotFound, "update.dataset.does-not-exist",
      "dataset" -> JString(datasetId))

  def contentTypeUnparsable(contentType: String) =
    err(BadRequest, "req.content-type.unparsable",
      "content-type" -> JString(contentType))

  def contentNotJson(contentType: MimeType) =
    err(UnsupportedMediaType, "req.content-type.not-json",
      "content-type" -> JString(contentType.toString))

  def contentUnknownCharset(contentType: MimeType) =
    err(UnsupportedMediaType, "req.content-type.unknown-charset",
      "content-type" -> JString(contentType.toString))

  val preconditionFailed = err(PreconditionFailed, "req.precondition-failed")
  val noContentType = err(BadRequest, "req.content-type.missing")
  val contentNotJsonArray = err(BadRequest, "req.body.not-json-array")

  def notModified(etags: Seq[EntityTag]) = etags.foldLeft(NotModified) { (resp, etag) => resp ~> ETag(etag) }

  // random errors that need formatting
  def badCopySelector(copy: String) = BadRequest ~> Content("Bad copy selector")
  def badOffset(offset: String) = BadRequest ~> Content("Bad offset")
  def badLimit(limit: String) = BadRequest ~> Content("Bad limit")
  val versionNotAllowed = BadRequest ~> Content("Can only change the schema of the latest copy")
}

object Errors extends StaticErrors {
}

class Errors(formatDatasetId: DatasetId => String) extends StaticErrors {
  import DataCoordinatorResource.{err, jsonifySchema}

  def mutationException(e: MutationException) = e match {
    case common: CommonMutationException => commonMutationError(common)
    case header: MutationScriptHeaderException => mutationScriptHeaderError(header)
    case upsert: UpsertException => upsertError(upsert)
    case ddl: DDLException => ddlError(ddl)
  }

  def commonMutationError(e: CommonMutationException) = {
    import CommonMutationException._
    e match {
      case CannotAcquireDatasetWriteLock(name) =>
        writeLockError(name)
      case SystemInReadOnlyMode() =>
        err(ServiceUnavailable, "update.read-only-mode")
    }
  }

  def mutationScriptHeaderError(e: MutationScriptHeaderException) = {
    import MutationScriptHeaderException._
    e match {
      case EmptyCommandStream() =>
        err(BadRequest, "req.script.header.missing")
      case HeaderIsNotAnObject(value) =>
        err(BadRequest, "req.script.header.non-object",
          "value" -> value)
      case NoSuchDataset(name) =>
        notFoundError(formatDatasetId(name))
      case MismatchedSchemaHash(name, schema) =>
        mismatchedSchema("req.script.header.mismatched-schema", name, schema)
      case MissingHeaderField(obj, field) =>
        err(BadRequest, "req.script.header.missing-field",
          "object" -> obj,
          "field" -> JString(field))
      case InvalidHeaderFieldValue(obj, field, value) =>
        err(BadRequest, "req.script.command.invalid-field",
          "object" -> obj,
          "field" -> JString(field),
          "value" -> value)
      case IncorrectLifecycleStage(name, currentStage, expectedStage) =>
        err(Conflict, "update.dataset.invalid-state",
          "dataset" -> JString(formatDatasetId(name)),
          "actual-state" -> JString(currentStage.name),
          "expected-state" -> JArray(expectedStage.toSeq.map(_.name).map(JString)))
      case InitialCopyDrop(name) =>
        err(Conflict, "update.dataset.initial-copy-drop",
          "dataset" -> JString(formatDatasetId(name)))
      case InvalidLocale(locale) =>
        err(BadRequest, "create.dataset.invalid-locale",
          "locale" -> JString(locale))
    }
  }

  def upsertHeaderError(e: MutationScriptHeaderException.Upsert) =
    mutationScriptHeaderError(e)

  def upsertError(e: UpsertException) = {
    import UpsertException._
    e match {
      case UnknownColumnId(datasetName, cid, index) =>
        err(BadRequest, "update.row.unknown-column",
          "dataset" -> JString(formatDatasetId(datasetName)),
          "column" -> JsonCodec.toJValue(cid),
          "index" -> JNumber(index))
      case InvalidValue(datasetName, userColumnId, typeName, value, index) =>
        err(BadRequest, "update.row.unparsable-value",
          "dataset" -> JString(formatDatasetId(datasetName)),
          "column" -> JsonCodec.toJValue(userColumnId),
          "type" -> JString(typeName.name),
          "value" -> value,
          "index" -> JNumber(index))
      case UpsertError(datasetName, NoPrimaryKey, _, index) =>
        err(BadRequest, "update.row.no-id",
          "dataset" -> JString(formatDatasetId(datasetName)),
          "index" -> JNumber(index))
      case UpsertError(datasetName, NoSuchRowToDelete(id), _, index) =>
        err(BadRequest, "update.row.no-such-id",
          "dataset" -> JString(formatDatasetId(datasetName)),
          "value" -> id,
          "index" -> JNumber(index))
      case UpsertError(datasetName, NoSuchRowToUpdate(id), _, index) =>
        err(BadRequest, "update.row.no-such-id",
          "dataset" -> JString(formatDatasetId(datasetName)),
          "value" -> id,
          "index" -> JNumber(index))
      case UpsertError(datasetName, VersionMismatch(id, expected, actual), rowVersionToJson, index) =>
        err(BadRequest, "update.row-version-mismatch",
          "dataset" -> JString(formatDatasetId(datasetName)),
          "value" -> id,
          "expected" -> expected.map(rowVersionToJson).getOrElse(JNull),
          "actual" -> actual.map(rowVersionToJson).getOrElse(JNull),
          "index" -> JNumber(index))
      case UpsertError(datasetName, VersionOnNewRow, _, index) =>
        err(BadRequest, "update.version-on-new-row",
          "dataset" -> JString(formatDatasetId(datasetName)),
          "index" -> JNumber(index))
    }
  }

  def ddlError(e: DDLException) = {
    import DDLException._

    def colErr(msg: String, dataset: DatasetId, colId: UserColumnId, resp: HttpResponse = BadRequest) = {
      err(resp, msg,
        "dataset" -> JString(formatDatasetId(dataset)),
        "column" -> JsonCodec.toJValue(colId),
        "index" -> JNumber(e.index))
    }

    e match {
      case CommandIsNotAnObject(value, index) =>
        err(BadRequest, "req.script.command.non-object",
          "value" -> value,
          "index" -> JNumber(index))
      case MissingCommandField(obj, field, index) =>
        err(BadRequest, "req.script.command.missing-field",
          "object" -> obj,
          "field" -> JString(field),
          "index" -> JNumber(index))
      case InvalidCommandFieldValue(obj, field, value, index) =>
        err(BadRequest, "req.script.command.invalid-field",
          "object" -> obj,
          "field" -> JString(field),
          "value" -> value,
          "index" -> JNumber(index))
      case NoSuchType(typeName, index) =>
        err(BadRequest, "update.type.unknown",
          "type" -> JString(typeName.name),
          "index" -> JNumber(index))
      case NoSuchColumn(dataset, name, _) =>
        colErr("update.column.not-found", dataset, name)
      case InvalidSystemColumnOperation(dataset, name, _, _) =>
        colErr("update.column.system", dataset, name)
      case PrimaryKeyAlreadyExists(datasetName, userColumnId, existingColumn, index) =>
        err(BadRequest, "update.row-identifier.already-set",
          "dataset" -> JString(formatDatasetId(datasetName)),
          "column" -> JsonCodec.toJValue(userColumnId),
          "existing-column" -> JsonCodec.toJValue(userColumnId),
          "index" -> JNumber(index))
      case InvalidTypeForPrimaryKey(datasetName, userColumnId, typeName, index) =>
        err(BadRequest, "update.row-identifier.invalid-type",
          "dataset" -> JString(formatDatasetId(datasetName)),
          "column" -> JsonCodec.toJValue(userColumnId),
          "type" -> JString(typeName.name),
          "index" -> JNumber(index))
      case DeleteRowIdentifierNotAllowed(datasetId, columnId, _) =>
        colErr("update.row-identifier.delete", datasetId, columnId)
      case DuplicateValuesInColumn(dataset, name, _) =>
        colErr("update.row-identifier.duplicate-values", dataset, name)
      case NullsInColumn(dataset, name, _) =>
        colErr("update.row-identifier.null-values", dataset, name)
      case NotPrimaryKey(dataset, name, _) =>
        colErr("update.row-identifier.not-row-identifier", dataset, name)
      case NoSuchColumnLabel(label, index) =>
        err(BadRequest, "req.script.command.no-such-label",
          "value" -> JString(label),
          "index" -> JNumber(index))
    }
  }

  def mismatchedSchema(code: String, name: DatasetId, schema: Schema) =
    err(Conflict, code,
      "dataset" -> JString(formatDatasetId(name)),
      "schema" -> jsonifySchema(schema))

  def writeLockError(datasetId: DatasetId) =
    err(Conflict, "update.dataset.temporarily-not-writable",
      "dataset" -> JString(formatDatasetId(datasetId)))
}
