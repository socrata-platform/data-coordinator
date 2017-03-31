package com.socrata.datacoordinator.secondary

case class DatasetInfo(internalName: String,
                       localeName: String,
                       obfuscationKey: Array[Byte],
                       resourceName: Option[String])
