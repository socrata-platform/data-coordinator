package com.socrata.querycoordinator

/**
 * Selects a Secondary Instance
 */
class SecondaryInstanceSelector {

  def getInstance(dataset:String, instanceHint:Option[String]) = {
    instanceHint.getOrElse("es.primus")
  }

}
