package com.socrata.datacoordinator.truth.metadata

import com.socrata.datacoordinator.id.{ColumnId, DatasetId}

class DatasetSystemIdAlreadyInUse(val datasetId: DatasetId) extends Exception(s"Dataset system id ${datasetId.underlying} already in use")
class DatasetAlreadyExistsException(val datasetId: String) extends Exception(s"Dataset `$datasetId' already exists")
class ColumnSystemIdAlreadyInUse(val table: VersionInfo, val columnId: ColumnId) extends Exception(s"Column system id ${columnId.underlying} already in use on dataset ${table.datasetInfo.datasetId}")
class ColumnAlreadyExistsException(val table: VersionInfo, columnName: String) extends Exception(s"Column `$columnName' already exists on dataset ${table.datasetInfo.datasetId}")
