package com.socrata.datacoordinator
package truth

import com.socrata.datacoordinator.id.DatasetId

class DatasetIdInUseByWriterException(val datasetId: DatasetId, cause: Throwable) extends Exception
class DatabaseInReadOnlyMode(cause: Throwable) extends Exception
