package com.socrata.datacoordinator
package truth.loader.sql

import com.socrata.datacoordinator.truth.loader.Logger

class NullLogger[A, B] extends Logger[A, B] {
  def columnCreated(name: String, typ: A) {}

  def columnRemoved(name: String) {}

  def rowIdentifierChanged(name: Option[String]) {}

  def workingCopyCreated() {}

  def workingCopyDropped() {}

  def workingCopyPublished() {}

  def endTransaction() = None

  def insert(systemID: Long, row: Row[B]) {}

  def update(sid: Long, row: Row[B]) {}

  def delete(systemID: Long) {}

  def close() {}
}

object NullLogger extends NullLogger[Any, Any] {
  def apply[A, B]() = this.asInstanceOf[NullLogger[A, B]]
}
