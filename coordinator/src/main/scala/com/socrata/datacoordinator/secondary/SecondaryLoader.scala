package com.socrata.datacoordinator.secondary

import scala.language.existentials

import java.io.{InputStreamReader, FilenameFilter, File}
import java.net.URLClassLoader
import com.rojoma.json.util.{JsonUtil, AutomaticJsonCodecBuilder, JsonKey}
import com.rojoma.json.io.JsonReaderException
import scala.collection.immutable.VectorBuilder

case class SecondaryDescription(@JsonKey("class") className: String)
object SecondaryDescription {
  implicit val jCodec = AutomaticJsonCodecBuilder[SecondaryDescription]
}

class SecondaryLoader(parentClassLoader: ClassLoader) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[SecondaryLoader])

  def loadSecondaries(dir: File): Seq[Secondary[_]] = {
    val jars = Option(dir.listFiles(new FilenameFilter {
      def accept(dir: File, name: String): Boolean = name.endsWith(".jar")
    })).getOrElse(Array.empty).toSeq
    val result = new VectorBuilder[Secondary[_]]
    for(jar <- jars) {
      val cl = new URLClassLoader(Array(jar.toURI.toURL), parentClassLoader)
      val stream = cl.getResourceAsStream("secondary-manifest.json")
      try {
        if(stream != null) {
            JsonUtil.readJson[SecondaryDescription](new InputStreamReader(stream, "UTF-8")) match {
              case Some(desc) =>
                val instance = try {
                  val cls = cl.loadClass(desc.className)
                  if(!classOf[Secondary[_]].isAssignableFrom(cls)) {
                    log.warn(cls + " does not name a subclass of Secondary")
                    None
                  } else {
                    Some(cls.newInstance().asInstanceOf[Secondary[_]])
                  }
                } catch {
                  case e: Exception =>
                    log.warn("Unable to create a new instance of " + desc.className, e)
                    None
                }
                instance.foreach(result += _)
              case None =>
                log.warn("Unable to parse a SecondaryDescription from " + jar.getAbsolutePath)
            }
        } else {
            log.warn("No secondary-manifest.json in " + jar.getAbsolutePath)
        }
      } catch {
        case e: JsonReaderException =>
          log.warn("Unable to parse " + jar.getAbsolutePath + " as JSON", e)
      } finally {
        if(stream != null) stream.close()
      }
    }
    result.result()
  }
}

case class LoadedSecondary(instance: Secondary[_])
