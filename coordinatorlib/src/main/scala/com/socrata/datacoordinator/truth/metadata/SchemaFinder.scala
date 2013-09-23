package com.socrata.datacoordinator.truth.metadata

trait SchemaFinder[CT] {
  def schemaHash(ctx: DatasetCopyContext[CT]): String
  def getSchema(ctx: DatasetCopyContext[CT]): Schema
}
