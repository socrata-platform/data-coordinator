package com.socrata.datacoordinator.truth.loader

trait Logger[CT, CV] extends DataLogger[CV] {
  def versionNum: Long
  def columnCreated(name: String, typ: CT)
  def columnRemoved(name: String)
  def rowIdentifierChanged(name: Option[String])
  def workingCopyCreated()
  def workingCopyDropped()
  def workingCopyPublished()
  def endTransaction()
}
