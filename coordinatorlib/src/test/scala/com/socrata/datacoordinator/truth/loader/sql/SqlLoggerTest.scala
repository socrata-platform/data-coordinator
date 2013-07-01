package com.socrata.datacoordinator.truth.loader.sql

import org.scalatest.matchers.MustMatchers
import org.scalatest.FunSuite

class SqlLoggerTest extends FunSuite with MustMatchers {
  test("All tags are short enough") {
    for(tag <- SqlLogger.allEvents) {
      tag.length must be <= SqlLogger.maxOpLength
    }
  }

  test("All tags are composed purely of lower-case characters") {
    for(tag <- SqlLogger.allEvents) {
      tag.foreach { c =>
        c must (be >= 'a' and be <= 'z')
      }
    }
  }
}
