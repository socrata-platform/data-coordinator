package com.socrata.datacoordinator.secondary

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.secondary.messaging._
import com.socrata.datacoordinator.truth.universe._

object SecondaryReplicationMessages {
  type SuperUniverse[CT, CV] = Universe[CT, CV] with DatasetMapReaderProvider with
                                                     SecondaryManifestProvider with
                                                     SecondaryStoresConfigProvider
}

class SecondaryReplicationMessages[CT, CV](u: SecondaryReplicationMessages.SuperUniverse[CT, CV],
                                           producer: MessageProducer) {

  def send(datasetId: DatasetId, storeId: String, endingDataVersion: Long, startingMillis: Long, endingMillis: Long): Unit = {
    // Here we are currently attempting to read from the following tables all in the same transaction:
    //  - dataset_map
    //  - secondary_manifest
    //  - secondary_stores_config
    //
    // We can probably get away with committing in between if we have to,
    // if this causes problems we can try that.
    val reader = u.datasetMapReader
    for {
      datasetInfo <- reader.datasetInfo(datasetId)
      copyInfo <- Some(reader.latestUpTo(datasetInfo, Some(endingDataVersion)))
      resourceName <- datasetInfo.resourceName
      groupName <- u.secondaryStoresConfig.group(storeId)
    } {
      val uid = ToViewUid(resourceName)

      // if the store has a group, send a replication event message for the whole group
      val stores = u.secondaryManifest.stores(datasetId).filterKeys {
        storeId => u.secondaryStoresConfig.group(storeId) == Some(groupName)
      }

      if (stores.forall { case (_, version) => version >= endingDataVersion}) {
        // it's okay if the others are ahead
        producer.send(GroupReplicationComplete(uid, groupName, stores.keySet,
          newDataVersion = endingDataVersion,
          endingAtMs = endingMillis,
          copyInfo.lifecycleStage
        ))
      }
    }
  }
}
