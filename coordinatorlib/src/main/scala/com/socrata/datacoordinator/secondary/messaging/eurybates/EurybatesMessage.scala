package com.socrata.datacoordinator.secondary.messaging.eurybates

import com.socrata.eurybates.Tag

import com.socrata.datacoordinator.secondary.messaging.{GroupReplicationComplete, Message, StoreReplicationComplete}

object EurybatesMessage {
  val SecondaryDataVersionUpdated = "SECONDARY_DATA_VERSION_UPDATED"
  val SecondaryGroupDataVersionUpdated = "SECONDARY_GROUP_DATA_VERSION_UPDATED"

  def tag(message: Message): Tag = message match {
    case _: StoreReplicationComplete => SecondaryDataVersionUpdated
    case _: GroupReplicationComplete => SecondaryGroupDataVersionUpdated
  }
}
