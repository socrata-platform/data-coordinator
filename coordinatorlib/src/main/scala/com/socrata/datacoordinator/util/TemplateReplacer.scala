package com.socrata.datacoordinator.util

import scala.{collection => sc}
import scala.annotation.tailrec

/** Simple string interpolation.  This replaces variables of the form %VARIABLE_NAME%
  * with other strings provided. */
object TemplateReplacer extends ((String, sc.Map[String, String]) => String) {
  def apply(source: String, env: sc.Map[String, String]): String = {
    @tailrec
    def loop(remaining: String, result: StringBuilder): String = {
      pattern.findFirstMatchIn(remaining) match {
        case Some(mtch) =>
          val replacement = env.getOrElse(mtch.group(1).toLowerCase, throw new UnboundVariableException(mtch.group(1)))
          result.append(remaining.substring(0, mtch.start)).append(replacement)
          loop(remaining.substring(mtch.end), result)
        case None =>
          if(result.isEmpty) remaining
          else result.append(remaining).toString
      }
    }
    loop(source, new StringBuilder)
  }

  private val pattern = "%([A-Za-z_]+)%".r
}

class UnboundVariableException(pattern: String) extends RuntimeException("Unbound variable " + pattern)
