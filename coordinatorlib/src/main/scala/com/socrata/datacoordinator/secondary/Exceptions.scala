package com.socrata.datacoordinator.secondary

import com.socrata.datacoordinator.id.DatasetId

class BadVersionIncrementException extends RuntimeException
class BadSubversionIncrementException extends RuntimeException
class UnknownDataset extends RuntimeException
class WorkingCopyUnsupported extends RuntimeException
class VersionUnfinished extends RuntimeException
class DatasetCopyLessThanDataVersionNotFound(datasetId: DatasetId, dataVersion: Option[Long]) extends RuntimeException {
  override def getMessage: String =
    s"Looked up a table for ${datasetId} < version ${dataVersion.getOrElse(Long.MaxValue)} but didn't find any copy info?"
}
