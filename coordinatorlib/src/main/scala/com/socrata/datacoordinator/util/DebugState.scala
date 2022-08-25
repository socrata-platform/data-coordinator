package com.socrata.datacoordinator.util

import java.util.concurrent.ConcurrentHashMap

object DebugState {
  private val explainUpsert = new ConcurrentHashMap[Long, AnyRef]

  def requestUpsertExplanation(threadId: Long): Unit = explainUpsert.put(threadId, this)

  def isUpsertExplanationRequested(threadId: Long): Boolean = explainUpsert.remove(threadId) ne null
}
