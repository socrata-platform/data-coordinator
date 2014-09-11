package com.socrata.dummysecondary

import com.socrata.datacoordinator.secondary._
import com.typesafe.config.Config
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.secondary.ColumnInfo
import scala.Some
import com.socrata.datacoordinator.secondary.DatasetInfo

class DummySecondary(config: Config) extends Secondary[Any, Any] {
  def shutdown() {}

  val wantsWorkingCopies: Boolean = config.getBoolean("wants-working-copies")

  /** The dataset has been deleted. */
  def dropDataset(datasetInternalName: String, cookie: Secondary.Cookie) {
    println("Deleted dataset " + datasetInternalName)
  }

  /**
   * @return The `dataVersion` of the latest copy this secondary has.  Should
   *         return 0 if this ID does not name a known dataset.
   */
  def currentVersion(datasetInternalName: String, cookie: Secondary.Cookie): Long =
    readLine("What version of " + datasetInternalName + "? (" + cookie + ") ").toLong

  /**
   * @return The `copyNumber` of the latest copy this secondary has.  Should
   *         return 0 if this ID does not name a known dataset.
   */
  def currentCopyNumber(datasetInternalName: String, cookie: Secondary.Cookie): Long =
    readLine("What copy of " + datasetInternalName + "? (" + cookie + ") ").toLong

  /**
   * @return The `copyNumber`s of all snapshot copies in this secondary.
   */
  def snapshots(datasetInternalName: String, cookie: Secondary.Cookie): Set[Long] =
    readLine("Copy numbers of all snapshot for " + datasetInternalName + "? (" + cookie + ") ").split(',').map(_.toLong).toSet

  /**
   * Order this secondary to drop a snapshot.  This should ignore the request
   * if the snapshot is already gone (but it should signal an error if the
   * copyNumber does not name a snapshot).
   */
  def dropCopy(datasetInternalName: String, copyNumber: Long, cookie: Secondary.Cookie): Secondary.Cookie =
    readLine("Dropping copy " + datasetInternalName + "; new cookie? (" + cookie + ") ") match {
      case "" => cookie
      case other => Some(other)
    }

  /** Provide the current copy an update.  The secondary should ignore it if it
    * already has this dataVersion.
    * @return a new cookie to store in the secondary map
    */
  def version(datasetInfo: DatasetInfo, dataVersion: Long, cookie: Secondary.Cookie, events: Iterator[Event[Any, Any]]): Secondary.Cookie = {
    println("Got a new version of " + datasetInfo.internalName)
    println("Version " + dataVersion)
    println("Current cookie: " + cookie)
    readLine("Skip or read or resync? ") match {
      case "skip" | "s" =>
        // pass
      case "read" | "r" =>
        while(events.hasNext) println(events.next())
      case "resync" =>
        ???
    }
    println("Current cookie: " + cookie)
    readLine("New cookie? ") match {
      case "" => cookie
      case other => Some(other)
    }
  }

  def resync(datasetInfo: DatasetInfo, copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo[Any]], cookie: Secondary.Cookie,
             rows: _root_.com.rojoma.simplearm.Managed[Iterator[com.socrata.datacoordinator.secondary.Row[Any]]],
             rollups: Seq[RollupInfo]): Secondary.Cookie = {
    println("Got a resync request on " + datasetInfo.internalName)
    println("Copy: " + copyInfo)
    println("Current cookie: " + cookie)
    readLine("Skip or read? ") match {
      case "skip" | "s" =>
        // pass
      case "read" | "r" =>
        rows.foreach { it =>
          it.foreach(println)
        }
        rollups.foreach(println)
    }
    println("Current cookie: " + cookie)
    readLine("New cookie? ") match {
      case "" => cookie
      case other => Some(other)
    }
  }
}
