package com.socrata.datacoordinator.truth.metadata

import com.socrata.datacoordinator.id.{UserColumnId, CopyId, ColumnId, DatasetId}

class DatasetSystemIdAlreadyInUse(val datasetId: DatasetId) extends Exception(s"Dataset system id ${datasetId.underlying} already in use")

class CopySystemIdAlreadyInUse(val copyId: CopyId) extends Exception(s"Copy system id ${copyId.underlying} already in use")

class ColumnSystemIdAlreadyInUse(val copy: CopyInfo, val columnId: ColumnId) extends Exception(s"Column system id ${columnId.underlying} already in use on dataset ${copy.datasetInfo.systemId}")
class ColumnAlreadyExistsException(val copy: CopyInfo, val columnId: UserColumnId) extends Exception(s"Column `${columnId.underlying}' already exists on dataset ${copy.datasetInfo.systemId}")
