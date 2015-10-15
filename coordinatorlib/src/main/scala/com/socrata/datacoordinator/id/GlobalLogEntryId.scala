package com.socrata.datacoordinator.id

class GlobalLogEntryId(val underlying: Long) extends AnyVal {
  override def toString: String = s"GlobalLogEntryId($underlying)"
}
