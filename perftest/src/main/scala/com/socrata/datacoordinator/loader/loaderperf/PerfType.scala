package com.socrata.datacoordinator.loader.loaderperf

sealed abstract class PerfType
case object PTId extends PerfType
case object PTNumber extends PerfType
case object PTText extends PerfType
