package com.socrata.datacoordinator.truth.metadata

import com.socrata.datacoordinator.id.{VersionId, ColumnId, DatasetId}

class DatasetSystemIdAlreadyInUse(val datasetId: DatasetId) extends Exception(s"Dataset system id ${datasetId.underlying} already in use")
class DatasetAlreadyExistsException(val datasetId: String) extends Exception(s"Dataset `$datasetId' already exists")

class VersionSystemIdAlreadyInUse(val versionId: VersionId) extends Exception(s"Version system id ${versionId.underlying} already in use")

class ColumnSystemIdAlreadyInUse(val table: VersionInfo, val columnId: ColumnId) extends Exception(s"Column system id ${columnId.underlying} already in use on dataset ${table.datasetInfo.datasetId}")
class ColumnAlreadyExistsException(val table: VersionInfo, columnName: String) extends Exception(s"Column `$columnName' already exists on dataset ${table.datasetInfo.datasetId}")
