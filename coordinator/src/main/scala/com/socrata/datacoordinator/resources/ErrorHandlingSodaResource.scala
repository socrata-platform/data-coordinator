package com.socrata.datacoordinator.resources

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.service.CoordinatorErrorsAndMetrics

class ErrorHandlingSodaResource(formatDatasetId: DatasetId => String)
  extends CoordinatorErrorsAndMetrics(formatDatasetId) with SodaResource
