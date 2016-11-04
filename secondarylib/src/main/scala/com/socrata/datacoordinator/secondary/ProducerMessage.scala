package com.socrata.datacoordinator.secondary

import com.rojoma.json.v3.util._
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.eurybates.Tag

sealed abstract class ProducerMessage

@JsonKeyStrategy(Strategy.Underscore)
case class StoreVersion(datasetSystemId: String,
                        groupName: Option[String],
                        storeId: String,
                        newDataVersion: Long,
                        startingAtMs: Long,
                        endingAtMs: Long) extends ProducerMessage

object StoreVersion {
  implicit val encode = AutomaticJsonEncodeBuilder[StoreVersion]

  def apply(datasetSystemId: DatasetId, groupName: Option[String], storeId: String,newDataVersion: Long,
            startingAtMs: Long, endingAtMs: Long): String => StoreVersion = { instance =>
    StoreVersion(WithInstance(instance, datasetSystemId), groupName, storeId, newDataVersion, startingAtMs, endingAtMs)
  }
}

@JsonKeyStrategy(Strategy.Underscore)
case class GroupVersion(datasetSystemId: String,
                        groupName: String,
                        storeIds: Set[String],
                        newDataVersion: Long,
                        endingAtMs: Long) extends ProducerMessage

object GroupVersion {
  implicit val encode = AutomaticJsonEncodeBuilder[GroupVersion]

  def apply(datasetSystemId: DatasetId, groupName: String, storeIds: Set[String], newDataVersion: Long,
            endingAtMs: Long): String => GroupVersion = { instance =>
    GroupVersion(WithInstance(instance, datasetSystemId), groupName, storeIds, newDataVersion, endingAtMs)

  }
}

object ProducerMessage {
  implicit val encode = SimpleHierarchyEncodeBuilder[ProducerMessage](NoTag)
    .branch[StoreVersion]
    .branch[GroupVersion]
    .build

  def tag(message: ProducerMessage): Tag = message match {
    case m: StoreVersion => "SECONDARY_DATA_VERSION_UPDATED"
    case m: GroupVersion => "SECONDARY_GROUP_DATA_VERSION_UPDATED"
  }
}

object WithInstance {
  def apply(instance: String, datasetSystemId: DatasetId): String =  s"$instance.${datasetSystemId.underlying}"
}
