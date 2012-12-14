package com.socrata.datacoordinator.util

trait LockProvider {
  def lock(name: String, timeoutMS: Long): Unlocker

  trait Unlocker {
    def unlock()
  }
}

class LockTimeoutException(val lock: String) extends RuntimeException
