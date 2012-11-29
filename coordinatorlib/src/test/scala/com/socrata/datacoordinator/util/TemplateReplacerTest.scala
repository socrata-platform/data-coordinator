package com.socrata.datacoordinator.util

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers

class TemplateReplacerTest extends FunSuite with MustMatchers {
  test("replace at start of string") {
    TemplateReplacer("%HELLO%, world", Map("hello" -> "world")) must equal ("world, world")
  }

  test("replace at end of string") {
    TemplateReplacer("Hello, %HELLO%", Map("hello" -> "world")) must equal ("Hello, world")
  }

  test("replace whole string") {
    TemplateReplacer("%HELLO%", Map("hello" -> "world")) must equal ("world")
  }

  test("multiple replaces") {
    TemplateReplacer("The canonical first program is %h%, %w%!", Map("h" -> "hello", "w" -> "world")) must equal ("The canonical first program is hello, world!")
  }

  test("Allows other percents") {
    TemplateReplacer("This: % is just a percent sign (%)", Map.empty) must equal ("This: % is just a percent sign (%)")
  }

  test("No changes must return the original string") {
    val x = "Blah"
    TemplateReplacer(x, Map.empty) must be theSameInstanceAs (x)
  }

  test("Unknown variable throws the right exception") {
    evaluating { TemplateReplacer("%x%", Map.empty) } must produce[UnboundVariableException]
  }
}
