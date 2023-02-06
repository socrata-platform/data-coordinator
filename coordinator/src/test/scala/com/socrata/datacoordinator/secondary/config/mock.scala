package com.socrata.datacoordinator.secondary.config.mock

import com.socrata.datacoordinator.secondary.config.{SecondaryGroupConfig => ISecondaryGroupConfig, StoreConfig => IStoreConfig}

case class StoreConfig(storeCapacityMB: Long, acceptingNewDatasets: Boolean) extends IStoreConfig

case class SecondaryGroupConfig(numReplicas: Int, instances: Map[String, StoreConfig], respectsCollocation: Boolean) extends ISecondaryGroupConfig

