package com.socrata.datacoordinator.truth.metadata

class DatasetAlreadyExistsException(val datasetId: String) extends Exception(s"Dataset `$datasetId' already exists")
class ColumnAlreadyExistsException(val table: VersionInfo, columnName: String) extends Exception(s"Column `$columnName' already exists on dataset ${table.datasetInfo.datasetId}")
