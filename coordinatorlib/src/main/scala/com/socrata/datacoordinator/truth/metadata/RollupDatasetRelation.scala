package com.socrata.datacoordinator.truth.metadata

import com.rojoma.json.v3.util.{AutomaticJsonCodec, AutomaticJsonCodecBuilder}
import com.socrata.datacoordinator.id.RollupName
import com.socrata.soql.environment.ResourceName

case class RollupDatasetRelation(primaryDataset: ResourceName,rollupName: RollupName,soql:String,secondaryDatasets:Set[ResourceName])

object RollupDatasetRelation{
  import com.socrata.datacoordinator.truth.json.ResourceNameCodec._
  implicit val jCodec = AutomaticJsonCodecBuilder[RollupDatasetRelation]
}
