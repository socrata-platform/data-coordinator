package com.socrata.datacoordinator.truth.metadata

trait SchemaFinder[CT] {
  def schemaHash(ctx: DatasetCopyContext[CT]): String
  def getSchema(ctx: DatasetCopyContext[CT]): Schema

  // TODO: getSchemaWithFieldName and getSchema coexists to facilitate join support transition.
  // Once transition is complete, keep only the one which always contains field names.
  def getSchemaWithFieldName(ctx: DatasetCopyContext[CT]): SchemaWithFieldName
}
