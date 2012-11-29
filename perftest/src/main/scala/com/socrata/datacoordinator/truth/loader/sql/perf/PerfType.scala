package com.socrata.datacoordinator
package truth.loader
package sql
package perf

sealed abstract class PerfType
case object PTId extends PerfType
case object PTNumber extends PerfType
case object PTText extends PerfType
