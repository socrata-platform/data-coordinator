package com.socrata.datacoordinator.common

import scala.concurrent.duration._
import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.metadata.{DatasetCopyContext, Schema, SchemaWithFieldName}
import com.socrata.datacoordinator.truth.universe.{CacheProvider, DatasetMapReaderProvider, Universe}
import com.socrata.datacoordinator.util.{Cache, RotateSchema}
import com.socrata.soql.environment.TypeName
import SchemaFinder._

class SchemaFinder[CT, CV](typeSerializer: CT => TypeName, cache: Cache) extends
    com.socrata.datacoordinator.truth.metadata.SchemaFinder[CT] {
  def schemaHash(ctx: DatasetCopyContext[CT]): String = {
    val key = List("schemahash", ctx.datasetInfo.systemId.underlying.toString, ctx.copyInfo.dataVersion.toString)
    cache.lookup[String](key) match {
      case Some(result) =>
        result
      case None =>
        val result = SchemaHash.computeHash(ctx.schema, ctx.datasetInfo.localeName, typeSerializer)
        cache.cache(key, result, 24.hours)
        result
    }
  }

  def getSchema(ctx: DatasetCopyContext[CT]): Schema = {
    val hash = schemaHash(ctx)
    val columns = RotateSchema(ctx.schema).mapValuesStrict { col => typeSerializer(col.typ) }
    val pk = ctx.pkCol_!.userColumnId
    Schema(hash, columns, pk, ctx.datasetInfo.localeName)
  }

  def getSchemaWithFieldName(ctx: DatasetCopyContext[CT]): SchemaWithFieldName = {
    val hash = schemaHash(ctx)
    val columns = RotateSchema(ctx.schema).mapValuesStrict { col => (typeSerializer(col.typ), col.fieldName) }
    val pk = ctx.pkCol_!.userColumnId
    SchemaWithFieldName(hash, columns, pk, ctx.datasetInfo.localeName)
  }
}

object SchemaFinder {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[SchemaFinder[_, _]])
}
