package com.socrata.datacoordinator.secondary

import scala.language.existentials

import java.nio.charset.StandardCharsets.UTF_8
import java.io.{Reader, InputStreamReader, FilenameFilter, File}
import java.net.URLClassLoader
import com.rojoma.json.util.{JsonUtil, AutomaticJsonCodecBuilder, JsonKey}
import com.rojoma.json.io.JsonReaderException
import scala.util.control.ControlThrowable
import com.typesafe.config._
import scala.collection.JavaConverters._
import scala.io.{Codec, Source}

case class SecondaryDescription(@JsonKey("class") className: String, name: String)
object SecondaryDescription {
  implicit val jCodec = AutomaticJsonCodecBuilder[SecondaryDescription]
}

class SecondaryLoader(parentClassLoader: ClassLoader, secondaryConfigRoot: Config) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[SecondaryLoader])

  def loadSecondaries(dir: File): Map[String, Secondary[_, _]] = {
    val jars = Option(dir.listFiles(new FilenameFilter {
      def accept(dir: File, name: String): Boolean = name.endsWith(".jar")
    })).getOrElse(Array.empty).toSeq
    jars.foldLeft(Map.empty[String, Secondary[_, _]]) { (acc, jar) =>
      log.info("Loading secondary from " + jar.getAbsolutePath)
      try {
        // Class loader for loading a specific secondary implementation.
        // This requires the parent class loader to recognize that a secondary implements
        // the secondary trait.
        val cl = new URLClassLoader(Array(jar.toURI.toURL), parentClassLoader)
        // Class loader for loading secondary-manifest.json resource only.
        // Don't inherit from data coordinator class loader so that it will not
        // be confused with other resources even when the parent is associated with
        // a specific secondary implementation jar for debugging.
        val resourceCl = new URLClassLoader(Array(jar.toURI.toURL), null)
        val stream = resourceCl.getResourceAsStream("secondary-manifest.json")
        if(stream == null) throw Nope("No secondary-manifest.json in " + jar.getAbsolutePath)
        val desc = withStreamResource(resourceCl, jar, "secondary-manifest.json") { reader =>
          try {
            JsonUtil.readJson[SecondaryDescription](reader).getOrElse {
              throw Nope("Unable to parse a SecondaryDescription from " + jar.getAbsolutePath)
            }
          } catch {
            case e: JsonReaderException =>
              throw Nope("Unable to parse " + jar.getAbsolutePath + " as JSON", e)
          }
        }

        val secondaryConfig =
          try { secondaryConfigRoot.getConfig(desc.name) }
          catch {
            case e: ConfigException.Missing => ConfigFactory.empty
            case e: ConfigException.WrongType => throw Nope("Configuration for " + desc.name + " is not a valid config")
          }

        // If this secondary type has an entry map, "instances" then load the secondary once for each
        // sub-configuration
        // "instances": {"primus" => {/* some config */}, "yetanother" => {/* some config*/}}
        val instanceConfigs:Map[String, Config] = if (secondaryConfig.hasPath("instances")) {
          val cs:java.util.Set[java.util.Map.Entry[String, ConfigValue]] = secondaryConfig.getConfig("instances").root().entrySet()
          val css = cs.asScala map {
            case (e:java.util.Map.Entry[String, ConfigValue]) =>
              e.getKey -> secondaryConfig.getConfig("instances").getConfig(e.getKey)
          }
          css.toMap
        } else {
          Map("" -> secondaryConfig)
        }

        val loadedInstances = instanceConfigs map {
          case ((instanceName:String, conf:Config))  =>
          val mergedConfig = secondaryConfig.withFallback(loadBaseConfig(cl, jar))
          val name:String = desc.name + { if (instanceName.isEmpty) "" else "."  + instanceName }
          log.info("Loading instance " + name)
          if(acc.contains(name)) throw Nope("A secondary named " + name + " already exists")
          val cls =
            try { cl.loadClass(desc.className) }
            catch { case e: Exception => throw Nope("Unable to load class " + desc.className + " from " + jar.getAbsolutePath, e) }
          if(!classOf[Secondary[_,_]].isAssignableFrom(cls)) throw Nope(desc.className + " is not a subclass of Secondary")
          val ctor =
            try { cls.getConstructor(classOf[Config]) }
            catch { case e: Exception => throw Nope("Unable to find constructor for " + desc.className + " from " + jar.getAbsolutePath, e) }
          log.info("Instantiating secondary \"" + name + "\" from " + jar.getAbsolutePath + " with configuration " + conf.root.render)
          val instance =
            try { ctor.newInstance(conf).asInstanceOf[Secondary[_,_]] }
            catch { case e: Exception => throw Nope("Unable to create a new instance of " + desc.className, e) }
          name -> instance
        }
        acc ++ loadedInstances
      } catch {
        case Nope(msg, null) => log.warn(msg); acc
        case Nope(msg, ex) => log.warn(msg, ex); acc
      }
    }
  }

  private def withStreamResource[T](cl: ClassLoader, jar: File, name: String)(f: Reader => T): T = {
    val stream = cl.getResourceAsStream(name)
    if(stream == null) throw Nope("No " + name + " in " + jar.getAbsolutePath)
    try {
      f(new InputStreamReader(stream, UTF_8))
    } finally {
      stream.close()
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

object SecondaryLoader {
  def load(secondaryConfigs: ConfigObject, dir: File): Map[String, Secondary[_,_]] =
    new SecondaryLoader(Thread.currentThread.getContextClassLoader, secondaryConfigs.toConfig).loadSecondaries(dir)
}

