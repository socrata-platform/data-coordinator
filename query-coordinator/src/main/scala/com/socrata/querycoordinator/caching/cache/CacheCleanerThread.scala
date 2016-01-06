package com.socrata.querycoordinator.caching.cache

import java.io.Closeable
import java.util.concurrent.{TimeUnit, Semaphore}

import scala.concurrent.duration.FiniteDuration

class CacheCleanerThread(cleanerProvider: CacheCleanerProvider, interval: FiniteDuration) extends Closeable {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[CacheCleanerThread])
  private val finished = new Semaphore(0)
  private val intervalMS = interval.toMillis

  private val worker = new Thread {
    setDaemon(true)
    setName("Cache cleaner")

    override def run(): Unit = {
      val cleaner = cleanerProvider.cleaner()
      while(!finished.tryAcquire(intervalMS, TimeUnit.MILLISECONDS)) {
        try {
          cleaner.clean()
        } catch {
          case e: Exception =>
            log.error("Unexpected exception from cleaner", e)
        }
      }
    }
  }

  def start() {
    worker.start()
  }

  def close(): Unit = {
    finished.release()
    worker.join()
  }
}
