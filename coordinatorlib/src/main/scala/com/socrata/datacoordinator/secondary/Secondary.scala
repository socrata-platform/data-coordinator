package com.socrata.datacoordinator
package secondary

import java.io.Closeable

trait Secondary[CT, CV] extends Closeable {
  /**
   * Returns the current version of the dataset in the store, if it exists.
   */
  def currentVersion(datasetID: DatasetId): Option[Long]

  /**
   * Creates an empty dataset with the given ID.  If the dataset already exists, this
   * does nothing (i.e., this operation is idempotent.)
   */
  def create(datasetID: DatasetId)

  /**
   * @param version The first version (of potentially several) to be created by this update operation.
   * @throws BadVersionIncrementException if the version greater than current version + 1
   * @throws UnknownDataset if this dataset has not been created on the store
   */
  def update(datasetID: DatasetId, version: Long): SecondaryWriter[CT, CV] // throws StoreDoesntKnowDataset

  /**
   * This is called if a full replacement is required.  The dataset should be created
   * if it does not exist.
   *
   * @param version the final version which will be present after this StoreWriter completes.
   */
  def replace(datasetID: DatasetId, schema: Map[ColumnId, CT], version: Long): SecondaryWriter[CT, CV]

  /**
   * Destroys the dataset.  If the dataset does not exist, this does nothing.
   */
  def remove(datasetID: DatasetId)
}
