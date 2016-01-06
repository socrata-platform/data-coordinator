package com.socrata.querycoordinator.caching.cache.file

import java.io._
import java.net.URLEncoder

import com.rojoma.simplearm.v2._
import com.socrata.querycoordinator.caching.cache.{ValueRef, CacheSession}
import com.socrata.util.io.StreamWrapper

import scala.concurrent.duration.FiniteDuration

class FileCacheSession(dir: File, updateATimeInterval: FiniteDuration, streamWrapper: StreamWrapper) extends CacheSession {
  private val updateATimeMS = updateATimeInterval.toMillis
  private val scope = new ResourceScope("FileCacheSession(" + dir + ")")

  private def filename(key: String) = new File(dir, "f" + URLEncoder.encode(key, "UTF-8"))

  override def find(key: String, rs: ResourceScope): Option[ValueRef] = {
    try {
      val fname = filename(key)
      val now = System.currentTimeMillis
      if(fname.lastModified < now - updateATimeMS) fname.setLastModified(now)
      val f = scope.open(new FileInputStream(fname))
      val fc = scope.open(f.getChannel, transitiveClose = List(f))
      Some(rs.open(new FileValueRef(fc, streamWrapper, scope)))
    } catch {
      case e: FileNotFoundException =>
        None
    }
  }

  override def create(key: String)(filler: OutputStream => Unit): Unit = {
    val temp = File.createTempFile("tmp", ".tmp", dir)
    try {
      using(new ResourceScope(key)) { rs =>
        val fos = rs.open(new FileOutputStream(temp))
        val wrapped = streamWrapper.wrapOutputStream(fos, rs)
        val buffered = new BufferedOutputStream(wrapped)
        filler(buffered)
      }
      temp.renameTo(filename(key))
    } finally {
      temp.delete()
    }
  }

  def close(): Unit = {
    scope.close()
  }
}
