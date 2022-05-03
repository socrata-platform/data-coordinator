package com.socrata.dummysecondary

import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.secondary._
import com.typesafe.config.Config
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.secondary.ColumnInfo

import scala.io.StdIn
import com.socrata.datacoordinator.secondary.DatasetInfo
import com.socrata.datacoordinator.truth.metadata.IndexDirective

class DummySecondary(config: Config) extends Secondary[Any, Any] {
  def shutdown(): Unit = {}

  /** The dataset has been deleted. */
  def dropDataset(datasetInternalName: String, cookie: Secondary.Cookie): Unit = {
    println("Deleted dataset " + datasetInternalName)
  }

  /**
   * @return The `dataVersion` of the latest copy this secondary has.  Should
   *         return 0 if this ID does not name a known dataset.
   */
  def currentVersion(datasetInternalName: String, cookie: Secondary.Cookie): Long =
    StdIn.readLine("What version of " + datasetInternalName + "? (" + cookie + ") ").toLong

  /**
   * @return The `copyNumber` of the latest copy this secondary has.  Should
   *         return 0 if this ID does not name a known dataset.
   */
  def currentCopyNumber(datasetInternalName: String, cookie: Secondary.Cookie): Long =
    StdIn.readLine("What copy of " + datasetInternalName + "? (" + cookie + ") ").toLong

  /**
   * Order this secondary to drop a snapshot.  This should ignore the request
   * if the snapshot is already gone (but it should signal an error if the
   * copyNumber does not name a snapshot).
   */
  def dropCopy(datasetInfo: DatasetInfo, copyInfo: CopyInfo, cookie: Secondary.Cookie, isLatestCopy: Boolean): Secondary.Cookie =
    StdIn.readLine("Dropping copy " + datasetInfo.internalName + "; new cookie? (" + cookie + ") ") match {
      case "" => cookie
      case other => Some(other)
    }

  /** Provide the current copy an update.  The secondary should ignore it if it
    * already has this dataVersion.
    * @return a new cookie to store in the secondary map
    */
  def version(datasetInfo: DatasetInfo, initialDataVersion: Long, finalDataVersion: Long, cookie: Secondary.Cookie,
              events: Iterator[Event[Any, Any]]): Secondary.Cookie = {
    println("Got a new version of " + datasetInfo.internalName)
    println("Version range " + initialDataVersion + "-" + finalDataVersion)
    println("Current cookie: " + cookie)
    StdIn.readLine("Skip or read or resync? ") match {
      case "skip" | "s" =>
        // pass
      case "read" | "r" =>
        while(events.hasNext) println(events.next())
      case "resync" =>
        ???
    }
    println("Current cookie: " + cookie)
    StdIn.readLine("New cookie? ") match {
      case "" => cookie
      case other => Some(other)
    }
  }

  def resync(datasetInfo: DatasetInfo, copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo[Any]], cookie: Secondary.Cookie,
             rows: Managed[Iterator[com.socrata.datacoordinator.secondary.Row[Any]]],
             rollups: Seq[RollupInfo], indexDirectives: Seq[IndexDirective[Any]], indexes: Seq[IndexInfo], isLatestCopy: Boolean): Secondary.Cookie = {
    println("Got a resync request on " + datasetInfo.internalName)
    println("Copy: " + copyInfo)
    println("Current cookie: " + cookie)
    StdIn.readLine("Skip or read? ") match {
      case "skip" | "s" =>
        // pass
      case "read" | "r" =>
        rows.run { it =>
          it.foreach(println)
        }
        rollups.foreach(println)
    }
    println("Current cookie: " + cookie)
    StdIn.readLine("New cookie? ") match {
      case "" => cookie
      case other => Some(other)
    }
  }
}
