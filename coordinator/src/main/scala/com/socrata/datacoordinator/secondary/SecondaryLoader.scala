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
import com.rojoma.simplearm.util._

case class SecondaryDescription(@JsonKey("class") className: String, name: String)
object SecondaryDescription {
  implicit val jCodec = AutomaticJsonCodecBuilder[SecondaryDescription]
}

class SecondaryLoader(parentClassLoader: ClassLoader, secondaryConfig: com.socrata.datacoordinator.service.SecondaryConfig) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[SecondaryLoader])

  def loadSecondaries(): Map[String, Secondary[_, _]] = {
    val dir = secondaryConfig.path
    val jars = Option(dir.listFiles(new FilenameFilter {
      def accept(dir: File, name: String): Boolean = name.endsWith(".jar")
    })).getOrElse(Array.empty).toSeq


    log.info("Loading secondary types...")
    val secondaryTypesMap = jars.foldLeft(Map.empty[String, File]) { (acc, jar) =>
      log.info("Investigating secondary in " + jar.getAbsolutePath)
      try {
        // Class loader for loading secondary-manifest.json resource only.
        // Don't inherit from data coordinator class loader so that it will not
        // be confused with other resources even when the parent is associated with
        // a specific secondary implementation jar for debugging.
        for (resourceCl <- managed(new URLClassLoader(Array(jar.toURI.toURL), null))) yield {
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
          if(acc.contains(desc.name)) throw Nope("A secondary type named " + desc.name + " already exists")

          log.info("Found secondary type " + desc.name)

          acc + (desc.name -> jar)
        }
      } catch {
      case Nope(msg, null) => log.warn(msg); acc
      case Nope(msg, ex) => log.warn(msg, ex); acc
    }
  }

    log.info("Loading secondary instances...")
    val secondaryMap = secondaryConfig.instances.foldLeft(Map.empty[String, Secondary[_,_]]) { case (acc, (instanceName, instanceConfig)) =>
      log.info("Loading secondary instance " + instanceName)
      try {
        val jar = secondaryTypesMap.get(instanceConfig.secondaryType).getOrElse {
          throw Nope("Unable to find secondary instance type " + instanceConfig.secondaryType)
        }

        val secondary = loadSecondary(jar, instanceConfig.secondaryConfig)
        acc + (instanceName -> secondary)
      } catch {
        case Nope(msg, null) => log.warn(msg); acc
        case Nope(msg, ex) => log.warn(msg, ex); acc
      }
    }

    secondaryConfig.groups.values.foreach { g =>
      g.instances.foreach { i =>
        secondaryMap.get(i).orElse(throw Nope("Unable to find instance " + i))
      }
    }

    secondaryMap
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
      ConfigFactory.parseString(text, ConfigParseOptions.defaults().setOriginDescription("secondary.conf")).resolve()
    } catch {
      case e: Exception => throw Nope("Unable to parse base config in " + jar.getAbsolutePath, e)
    } finally {
      stream.close()
    }
  }

  private def loadSecondary(jar: File, config: Config): Secondary[_, _] = {
    val cl = new URLClassLoader(Array(jar.toURI.toURL), parentClassLoader)
    val stream = cl.getResourceAsStream("secondary-manifest.json")
    if(stream == null) throw Nope("No secondary-manifest.json in " + jar.getAbsolutePath)
    val desc = withStreamResource(cl, jar, "secondary-manifest.json") { reader =>
      try {
        JsonUtil.readJson[SecondaryDescription](reader).getOrElse {
          throw Nope("Unable to parse a SecondaryDescription from " + jar.getAbsolutePath)
        }
      } catch {
        case e: JsonReaderException =>
          throw Nope("Unable to parse " + jar.getAbsolutePath + " as JSON", e)
      }
    }

    val mergedConfig = config.withFallback(loadBaseConfig(cl, jar))

    val cls =
      try { cl.loadClass(desc.className) }
      catch { case e: Exception => throw Nope("Unable to load class " + desc.className + " from " + jar.getAbsolutePath, e) }
    if(!classOf[Secondary[_,_]].isAssignableFrom(cls)) throw Nope(desc.className + " is not a subclass of Secondary")
    val ctor =
      try { cls.getConstructor(classOf[Config]) }
      catch { case e: Exception => throw Nope("Unable to find constructor for " + desc.className + " from " + jar.getAbsolutePath, e) }
    log.info("Instantiating secondary type \"" + desc.name + "\" from " + jar.getAbsolutePath + " with configuration " + mergedConfig.root.render)

    try { ctor.newInstance(mergedConfig).asInstanceOf[Secondary[_,_]] }
    catch { case e: Exception => throw Nope("Unable to create a new instance of " + desc.className, e) }
  }

  private case class Nope(message: String, cause: Throwable = null) extends Throwable(message, cause) with ControlThrowable
}

object SecondaryLoader {
  def load(secondaryConfig: com.socrata.datacoordinator.service.SecondaryConfig): Map[String, Secondary[_,_]] =
    new SecondaryLoader(Thread.currentThread.getContextClassLoader, secondaryConfig).loadSecondaries()
}

