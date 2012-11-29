package com.socrata.datacoordinator.truth.sql
package sample

sealed abstract class SampleValue
case object SampleNull extends SampleValue
case class SampleSid(id: Long) extends SampleValue
case class SampleText(text: String) extends SampleValue
case class SamplePoint(x: Double, y: Double) extends SampleValue
