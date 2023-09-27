package com.socrata.datacoordinator.secondary

case class RollupInfo(name: String, soql: String) {
  def isNewAnalyzer = soql.startsWith("{")
}
