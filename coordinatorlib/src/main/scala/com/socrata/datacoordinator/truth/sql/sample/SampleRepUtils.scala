package com.socrata.datacoordinator.truth.sql.sample

import java.lang.StringBuilder

abstract class SampleRepUtils {
  def csvescape(sb: StringBuilder, s: Seq[Option[String]]): StringBuilder = {
    // this is example code; here we'd CSVify the given strings
    sb
  }
}
