package com.socrata.datacoordinator.packets.network

import java.nio.channels.SocketChannel
import java.io._
import java.lang.reflect.Field

import com.rojoma.simplearm.util._
import com.sun.jna.{Pointer, Native, Library}
import scala.Some

final abstract class KeepaliveSetup

/** Uggggh... Java doesn't seem to provide a nice way of doing low-level things
  * like custom sockopts.  This would be TOTALLY trivial to do if only Java
  * provided the right interface.  BUT NO....
  *
  * So instead, reflect out file descriptor that I need, pass it to a native library,
  * and hope for the best.*/
object KeepaliveSetup extends (SocketChannel => Unit) {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[KeepaliveSetup])

  private def channelField(channelClass: Class[_]) = try {
    val field = channelClass.getDeclaredField("fd")
    field.setAccessible(true)
    if(field.getType == classOf[FileDescriptor]) field
    else null
  } catch {
    case _: Throwable => null
  }

  private val fileDescriptorField = try {
    val cls = classOf[FileDescriptor]
    val field = cls.getDeclaredField("fd")
    field.setAccessible(true)
    if(field.getType == classOf[Int]) field
    else null
  } catch {
    case _: Throwable => null
  }

  private val canHasLib = sys.props("os.name") == "Linux" && sys.props("os.arch") == "amd64"

  private trait Lib extends Library {
    def platform_setup_keepalive(fd: Int, keepidle: Int, keepintvl: Int, keepcnt: Int): Pointer
    def platform_error_free(ptr: Pointer)
  }

  private val lib = if(canHasLib) {
    val tmp = File.createTempFile("fdlib",".so")
    try {
      for {
        os <- managed(new FileOutputStream(tmp))
        is <- managed(Option(getClass.getResourceAsStream("native-library")).getOrElse(throw new FileNotFoundException()))
      } {
        val buf = new Array[Byte](1024)
        def loop() {
          is.read(buf) match {
            case -1 => // done
            case n => os.write(buf, 0, n); loop()
          }
        }
        loop()
      }
      Native.loadLibrary(tmp.getAbsolutePath, classOf[Lib]).asInstanceOf[Lib]
    } catch {
      case _: IOException => null
    } finally {
      tmp.delete()
    }
  } else null

  private val hasLib = lib != null

  // Not sure how to make this hold onto the classes weakly; just a WeakHashMap
  // is not sufficient, because the Field has a strong reference to the class
  // object.
  private val fieldCache = new java.util.HashMap[Class[_], Option[Field]]()
  private def lookupField(cls: Class[_]): Option[Field] = {
    fieldCache.synchronized {
      val cached = fieldCache.get(cls)
      if(cached != null) cached
      else {
        val result = Option(channelField(cls))
        fieldCache.put(cls, result)
        result
      }
    }
  }

  def apply(channel: SocketChannel) {
    def warn() {
      log.warn("Unable to set keepalive settings; this could be because this isn't Linux/amd64, or because loading the native library failed, or because the structure of the classes we're grovelling through reflectively have changed.")
    }

    if(hasLib && fileDescriptorField != null) {
      lookupField(channel.getClass) match {
        case Some(field) =>
          val fdValue = field.get(channel)
          val fdValueValue = fileDescriptorField.get(fdValue)
          val fd = fdValueValue.asInstanceOf[java.lang.Integer].intValue
          val result = lib.platform_setup_keepalive(fd, 30, 30, 3)
          if(result != null) {
            try {
              val str = result.getString(0)
              throw new IOException(str)
            } finally {
              lib.platform_error_free(result)
            }
          }
        case None =>
          warn()
      }
    } else {
      warn()
    }
  }
}
