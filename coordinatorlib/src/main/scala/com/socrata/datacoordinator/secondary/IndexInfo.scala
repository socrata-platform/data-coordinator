package com.socrata.datacoordinator.secondary

import com.socrata.datacoordinator.id.IndexId

case class IndexInfo(systemId: IndexId, name: String, expressions: String, filter: Option[String])