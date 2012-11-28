package com.socrata.datacoordinator.truth.sample

sealed abstract class SampleType
case object SampleSidColumn extends SampleType
case object SampleTextColumn extends SampleType
case object SamplePointColumn extends SampleType
