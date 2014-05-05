package com.socrata.datacoordinator.service.mutator

import com.rojoma.json.ast.{JObject, JValue}
import com.rojoma.json.codec.JsonCodec
import com.socrata.datacoordinator.truth.universe.{SchemaFinderProvider, Universe}
import com.socrata.datacoordinator.truth.metadata.DatasetCopyContext
import com.socrata.datacoordinator.truth.{DatabaseInReadOnlyMode, DatasetIdInUseByWriterException}

object MutatorLib {
  def checkHash[CT, CV](u: Universe[CT, CV] with SchemaFinderProvider, schemaHash: Option[String], ctx: DatasetCopyContext[CT]) {
    for(givenSchemaHash <- schemaHash) {
      val realSchemaHash = u.schemaFinder.schemaHash(ctx)
      if(givenSchemaHash != realSchemaHash) {
        throw MutationScriptHeaderException.MismatchedSchemaHash(ctx.datasetInfo.systemId, u.schemaFinder.getSchema(ctx))
      }
    }
  }

  def translatingCannotWriteExceptions[T](f: => T) = {
    import CommonMutationException._
    try {
      f
    } catch {
      case e: DatasetIdInUseByWriterException =>
        throw CannotAcquireDatasetWriteLock(e.datasetId)
      case e: DatabaseInReadOnlyMode =>
        throw SystemInReadOnlyMode()
    }
  }

  trait Accessor {
    def originalObject: JObject
    def fields: scala.collection.Map[String, JValue] = originalObject.fields
    def get[T: JsonCodec](field: String): T
    def getOption[T: JsonCodec](field: String): Option[T]
    def getWithStrictDefault[T: JsonCodec](field: String, default: T): T
    def getWithLazyDefault[T: JsonCodec](field: String, default: => T): T
  }

  trait OnError {
    def missingField(obj: JObject, field: String): Nothing
    def invalidValue(obj: JObject, field: String, value: JValue): Nothing
    def notAnObject(value: JValue): Nothing
  }

  val HeaderError = new OnError {
    import MutationScriptHeaderException._

    override def missingField(obj: JObject, field: String): Nothing =
      throw MissingHeaderField(obj, field)

    override def invalidValue(obj: JObject, field: String, value: JValue): Nothing =
      throw InvalidHeaderFieldValue(obj, field, value)

    override def notAnObject(value: JValue): Nothing =
      throw HeaderIsNotAnObject(value)
  }

  def withObjectFields[A](value: JValue, onError: OnError)(f: Accessor => A): A = value match {
    case obj: JObject =>
      f(new Accessor {
        val originalObject = obj
        def get[T : JsonCodec](field: String) = {
          val json = fields.getOrElse(field, onError.missingField(originalObject, field))
          JsonCodec[T].decode(json).getOrElse(onError.invalidValue(originalObject, field, value))
        }
        def getWithStrictDefault[T : JsonCodec](field: String, default: T) = getWithLazyDefault(field, default)
        def getWithLazyDefault[T : JsonCodec](field: String, default: => T) = {
          fields.get(field) match {
            case Some(json) =>
              JsonCodec[T].decode(json).getOrElse(onError.invalidValue(originalObject, field, json))
            case None =>
              default
          }
        }
        def getOption[T : JsonCodec](field: String) =
          fields.get(field).map { json =>
            JsonCodec[T].decode(json).getOrElse(onError.invalidValue(originalObject, field, json))
          }
      })
    case other =>
      onError.notAnObject(other)
  }
}
