package com.socrata.datacoordinator.truth.metadata

import com.socrata.datacoordinator.id.{CopyId, ColumnId, DatasetId}

class DatasetSystemIdAlreadyInUse(val datasetId: DatasetId) extends Exception(s"Dataset system id ${datasetId.underlying} already in use")
class DatasetAlreadyExistsException(val datasetId: String) extends Exception(s"Dataset `$datasetId' already exists")

class CopySystemIdAlreadyInUse(val copyId: CopyId) extends Exception(s"Copy system id ${copyId.underlying} already in use")

class ColumnSystemIdAlreadyInUse(val copy: CopyInfo, val columnId: ColumnId) extends Exception(s"Column system id ${columnId.underlying} already in use on dataset ${copy.datasetInfo.datasetName}")
class ColumnAlreadyExistsException(val copy: CopyInfo, columnName: String) extends Exception(s"Column `$columnName' already exists on dataset ${copy.datasetInfo.datasetName}")
