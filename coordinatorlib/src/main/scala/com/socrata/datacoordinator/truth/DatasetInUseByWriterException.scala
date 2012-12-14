package com.socrata.datacoordinator
package truth

sealed abstract class DatasetInUseByWriterException(name: Any, cause: Throwable)
  extends RuntimeException("Dataset " + name + " is in use by another writer", cause)

class DatasetIdInUseByWriterException(val datasetId: String, cause: Throwable)
  extends DatasetInUseByWriterException(datasetId, cause)

class DatasetSystemIdInUseByWriterException(val datasetId: DatasetId, cause: Throwable)
  extends DatasetInUseByWriterException(datasetId, cause)
