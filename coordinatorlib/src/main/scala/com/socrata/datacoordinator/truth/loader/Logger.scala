package com.socrata.datacoordinator.truth.loader

trait Logger[CT, CV] extends DataLogger[CV] {
  def columnCreated(name: String, typ: CT)
  def columnRemoved(name: String)
  def rowIdentifierChanged(name: Option[String])
  def workingCopyCreated()
  def workingCopyDropped()
  def workingCopyPublished()

  /** Logs the end of the transaction and returns its version number.
   * @return The new log version number, or None if no other method was called. */
  def endTransaction(): Option[Long]
}
