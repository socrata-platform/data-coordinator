package com.socrata.datacoordinator.secondary.messaging.eurybates

import com.socrata.eurybates.Tag

import com.socrata.datacoordinator.secondary.messaging.{Message, StoreVersion, GroupVersion}

object EurybatesMessage {
  val SecondaryDataVersionUpdated = "SECONDARY_DATA_VERSION_UPDATED"
  val SecondaryGroupDataVersionUpdated = "SECONDARY_GROUP_DATA_VERSION_UPDATED"

  def tag(message: Message): Tag = message match {
    case _: StoreVersion => SecondaryDataVersionUpdated
    case _: GroupVersion => SecondaryGroupDataVersionUpdated
  }
}
