package com.socrata.datacoordinator.service

import scala.concurrent.duration._

import com.rojoma.simplearm.Managed

import com.socrata.datacoordinator.truth.universe.{CacheProvider, DatasetMapReaderProvider, Universe}
import com.socrata.datacoordinator.truth.metadata.DatasetCopyContext
import com.socrata.datacoordinator.util.{Cache, RotateSchema}
import com.socrata.soql.environment.TypeName
import com.socrata.datacoordinator.id.DatasetId

import SchemaFinder._

class SchemaFinder[CT, CV](universe: Managed[Universe[CT, CV] with DatasetMapReaderProvider with CacheProvider], typeSerializer: CT => TypeName) {
  def getSchema(datasetId: DatasetId): Option[Schema] =
    for {
      u <- universe
      dsInfo <- u.datasetMapReader.datasetInfo(datasetId)
    } yield {
      val latest = u.datasetMapReader.latest(dsInfo)
      val schema = u.datasetMapReader.schema(latest)
      val ctx = new DatasetCopyContext(latest, schema)
      getSchema(ctx, u.cache)
    }

  def schemaHash(ctx: DatasetCopyContext[CT], cache: Cache): String = {
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

  def getSchema(ctx: DatasetCopyContext[CT], cache: Cache): Schema = {
    val hash = schemaHash(ctx, cache)
    val columns = RotateSchema(ctx.schema).mapValuesStrict { col => typeSerializer(col.typ) }
    val pk = ctx.pkCol_!.userColumnId
    Schema(hash, columns, pk, ctx.datasetInfo.localeName)
  }
}

object SchemaFinder {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[SchemaFinder[_, _]])
}
