package com.socrata.datacoordinator.service.collocation

import java.util.concurrent.TimeUnit

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.locks.InterProcessMutex

sealed abstract class CollocationLockException(message: String, cause: Exception) extends Exception(message, cause)

case class CollocationLockTimeout(millis: Long)
  extends CollocationLockException(s"Timeout while waiting to acquire collocation lock after $millis milliseconds", null)
case class CollocationLockError(action: String, cause: Exception)
  extends CollocationLockException(s"Unexpected exception while $action collocation lock", cause)

trait CollocationLock {
  def acquire(timeoutMillis: Long): Boolean
  def release(): Unit
}

class CuratedCollocationLock(curator: CuratorFramework, lockPath: String) extends CollocationLock {
  val lock = new InterProcessMutex(curator, s"/${curator.getNamespace}$lockPath")

  override def acquire(timeoutMillis: Long): Boolean = {
    try {
      lock.acquire(timeoutMillis, TimeUnit.MILLISECONDS)
    } catch {
      case e: Exception => throw CollocationLockError("acquiring", e)
    }
  }

  override def release(): Unit = try {
    lock.release()
  } catch {
    case e: Exception => throw CollocationLockError("releasing", e)
  }
}
