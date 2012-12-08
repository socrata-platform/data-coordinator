package com.socrata.datacoordinator.truth.metadata

class DatasetInUseByWriterException(val datasetId: String, cause: Throwable)
  extends RuntimeException("Dataset " + datasetId + " is in use by another writer", cause)
