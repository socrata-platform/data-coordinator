package com.socrata.querycoordinator.caching.cache.file

import java.io.File

import com.socrata.querycoordinator.caching.cache.CacheCleaner

import scala.concurrent.duration.FiniteDuration

class FileCacheCleaner(dir: File, survivorCutoff: FiniteDuration, assumeDeadCreateCutoff: FiniteDuration) extends CacheCleaner {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[FileCacheCleaner])
  private val survivorCutoffMS = survivorCutoff.toMillis
  private val assumeDeadCreateCutoffMS = assumeDeadCreateCutoff.toMillis

  override def clean(): Unit = {
    val mainCutoff = System.currentTimeMillis - survivorCutoffMS
    val brokenCutoff = System.currentTimeMillis - assumeDeadCreateCutoffMS
    for {
      files <- Option(dir.listFiles())
      file <- files
      if file.isFile
    } {
      val lm = file.lastModified
      val doDelete =
        if(file.getName.startsWith("f")) lm < mainCutoff
        else if(file.getName.startsWith("tmp")) lm < brokenCutoff
        else false
      if(doDelete) {
        log.info("Deleting {}", file)
        file.delete()
      }
    }
  }
}
