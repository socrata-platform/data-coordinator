package com.socrata.datacoordinator.util

import scala.{collection => sc}
import scala.util.matching.Regex

/** Simple string interpolation.  This replaces variables of the form %VARIABLE_NAME%
  * with other strings provided. */
object TemplateReplacer extends ((String, sc.Map[String, String]) => String) {
  def apply(source: String, env: sc.Map[String, String]): String = {
    def replacer(mtch: Regex.Match) =
      env.getOrElse(mtch.group(1).toLowerCase, throw new UnboundVariableException(mtch.group(1)))
    pattern.replaceAllIn(source, replacer _)
  }

  private val pattern = "%([A-Za-z_]+)%".r
}

class UnboundVariableException(pattern: String) extends RuntimeException("Unbound variable " + pattern)
