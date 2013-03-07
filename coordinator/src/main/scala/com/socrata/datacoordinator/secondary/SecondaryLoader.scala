package com.socrata.datacoordinator.secondary

import scala.language.existentials

import java.io.{InputStreamReader, FilenameFilter, File}
import java.net.URLClassLoader
import com.rojoma.json.util.{JsonUtil, AutomaticJsonCodecBuilder, JsonKey}
import com.rojoma.json.io.JsonReaderException
import scala.util.control.ControlThrowable
import com.typesafe.config.{ConfigParseOptions, ConfigFactory, ConfigException, Config}
import scala.io.{Codec, Source}

case class SecondaryDescription(@JsonKey("class") className: String, name: String)
object SecondaryDescription {
  implicit val jCodec = AutomaticJsonCodecBuilder[SecondaryDescription]
}

class SecondaryLoader(parentClassLoader: ClassLoader, secondaryConfigRoot: Config) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[SecondaryLoader])

  def loadSecondaries(dir: File): Map[String, Secondary[_]] = {
    val jars = Option(dir.listFiles(new FilenameFilter {
      def accept(dir: File, name: String): Boolean = name.endsWith(".jar")
    })).getOrElse(Array.empty).toSeq
    jars.foldLeft(Map.empty[String, Secondary[_]]) { (acc, jar) =>
      log.info("Loading secondary from " + jar.getAbsolutePath)
      try {
        val cl = new URLClassLoader(Array(jar.toURI.toURL), parentClassLoader)
        val stream = cl.getResourceAsStream("secondary-manifest.json")
        if(stream == null) throw Nope("No secondary-manifest.json in " + jar.getAbsolutePath)
        try {
          val desc = try {
            JsonUtil.readJson[SecondaryDescription](new InputStreamReader(stream, "UTF-8")).getOrElse {
              throw Nope("Unable to parse a SecondaryDescription from " + jar.getAbsolutePath)
            }
          } catch {
              case e: JsonReaderException =>
                throw Nope("Unable to parse " + jar.getAbsolutePath + " as JSON", e)
          }
          val secondaryConfig =
            try { secondaryConfigRoot.getConfig(desc.name) }
            catch { case e: ConfigException => ConfigFactory.empty }
          val mergedConfig = secondaryConfig.withFallback(loadBaseConfig(cl, jar))
          if(acc.contains(desc.name)) throw Nope("A secondary named " + desc.name + " already exists")
          val cls =
            try { cl.loadClass(desc.className) }
            catch { case e: Exception => throw Nope("Unable to load class " + desc.className + " from " + jar.getAbsolutePath, e) }
          if(!classOf[Secondary[_]].isAssignableFrom(cls)) throw Nope(desc.className + " is not a subclass of Secondary")
          val ctor =
            try { cls.getConstructor(classOf[Config]) }
            catch { case e: Exception => throw Nope("Unable to find constructor for " + desc.className + " from " + jar.getAbsolutePath, e) }
          val instance =
            try { ctor.newInstance(mergedConfig).asInstanceOf[Secondary[_]] }
            catch { case e: Exception => throw Nope("Unable to create a new instance of " + desc.className, e) }
          acc + (desc.name -> instance)
        } finally {
          stream.close()
        }
      } catch {
        case Nope(msg, null) => log.warn(msg); acc
        case Nope(msg, ex) => log.warn(msg, ex); acc
      }
    }
  }

  private def loadBaseConfig(cl: ClassLoader, jar: File): Config = {
    val stream = cl.getResourceAsStream("secondary.conf")
    if(stream == null) ConfigFactory.empty
    else try {
      val text = Source.fromInputStream(stream)(Codec.UTF8).getLines().mkString("\n")
      ConfigFactory.parseString(text, ConfigParseOptions.defaults().setOriginDescription("secondary.conf"))
    } catch {
      case e: Exception => throw Nope("Unable to parse base config in " + jar.getAbsolutePath, e)
    } finally {
      stream.close()
    }
  }

  private case class Nope(message: String, cause: Throwable = null) extends Throwable(message, cause) with ControlThrowable
}
