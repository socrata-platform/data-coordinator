package com.socrata.querycoordinator

import com.socrata.thirdparty.typesafeconfig.ConfigClass

trait SecondarySelectorConfig extends ConfigClass {

  val allSecondaryInstanceNames: Seq[String] = getStringList("all-secondary-instance-names")

  val secondaryDiscoveryExpirationMillis: Long = getDuration("secondary-discovery-expiration").toMillis

  val datasetMaxNopeCount: Int = getInt("dataset-max-nope-count")
}
