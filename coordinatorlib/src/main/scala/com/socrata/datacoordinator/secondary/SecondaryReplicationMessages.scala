package com.socrata.datacoordinator.secondary

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.secondary.messaging._
import com.socrata.datacoordinator.truth.universe._
import com.typesafe.scalalogging.slf4j.Logging

object SecondaryReplicationMessages {
  type SuperUniverse[CT, CV] = Universe[CT, CV] with DatasetMapReaderProvider with
                                                     SecondaryManifestProvider with
                                                     SecondaryStoresConfigProvider
}

class SecondaryReplicationMessages[CT, CV](u: SecondaryReplicationMessages.SuperUniverse[CT, CV],
                                           producer: Producer) extends Logging {

  def send(datasetId: DatasetId, storeId: String, endingDataVersion: Long, startingMillis: Long, endingMillis: Long): Unit = {
    u.datasetMapReader.datasetInfo(datasetId).foreach { _.resourceName.foreach { resourceName =>
      val uid = ToViewUid(resourceName)

      val group = u.secondaryStoresConfig.group(storeId)
      // send replication event message for the secondary store
      producer.send(StoreVersion(uid,
        groupName = group,
        storeId = storeId,
        newDataVersion = endingDataVersion,
        startingAtMs = startingMillis,
        endingAtMs = endingMillis
      ))

      // if the store has a group, send a replication event message for the whole group
      group.foreach { name =>
        val stores = u.secondaryManifest.stores(datasetId).filterKeys {
          storeId => u.secondaryStoresConfig.group(storeId) == group
        }


        if (stores.forall { case (_, version) => version >= endingDataVersion}) {
          // it's okay if the others are ahead
          producer.send(GroupVersion(uid,
            groupName = name,
            storeIds = stores.keySet,
            newDataVersion = endingDataVersion,
            endingAtMs = endingMillis
          ))
        }
      }
    }}
  }
}
