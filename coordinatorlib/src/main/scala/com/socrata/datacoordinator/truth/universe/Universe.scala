package com.socrata.datacoordinator.truth.universe

import com.rojoma.simplearm.Managed

import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth._
import com.socrata.datacoordinator.util.{TransferrableContextTimingReport, RowIdProvider, TimingReport}
import com.socrata.datacoordinator.secondary.{SecondaryConfig, PlaybackToSecondary, SecondaryManifest}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import org.joda.time.DateTime

// Not sure I'll need all of these!  Certainly not all of them are implemented.
// The idea behind these traits is that they encapsulate "things which need a Connection".
// In order to hide the Connection from user code, they will all live in a single joined-up
// object.  In order to allow fine-grained access, client code can specify exactly which
// bits they want.  In theory there will be only one concrete implementing class at runtime,
// so hotspot ought to be able to resolve all these calls to non-virtual direct calls...

trait TypeUniverse {
  type CT
  type CV
}

/**
 * This both provides generic utility stuff and acts as a anchor-point
 * for the type system to match up `CT`/`CV` with other systems' `CT`/`CV`.
 * The general pattern will be to take a `Managed[Universe[A,B] with This with That with TheOther]`
 * and then access it like
 *     for { u <- universe } yield {
 *       import u._
 *       // ...use the various methods of the individual traits...
 *     }
 * Possibly some sub-calls would be passed `u` directly.
 */
trait Universe[ColumnType, ColumnValue] extends TypeUniverse {
  type CT = ColumnType
  type CV = ColumnValue

  /** Commit the current transaction, if this `Universe` is transactional.
    * Otherwise, does nothing.
    * @note If this `Universe` was received via the resource-management system,
    *       that system is responsible for committing on normal completion of
    *       the management block.  As a result, most code should not need to
    *       explicitly call this method.
    */
  def commit()

  val timingReport: TransferrableContextTimingReport

  def transactionStart: DateTime
}

trait LoggerProvider { this: TypeUniverse =>
  def logger(datasetInfo: DatasetInfo): Logger[CV]
}

trait DeloggerProvider { this: TypeUniverse =>
  def delogger(datasetInfo: DatasetInfo): Delogger[CV]
}

trait PrevettedLoaderProvider { this: TypeUniverse =>
  def prevettedLoader(copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo], logger: Logger[CV]): PrevettedLoader[CV]
}

trait LoaderProvider { this: TypeUniverse =>
  def loader(copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo], rowIdProvider: RowIdProvider, logger: Logger[CV]): Managed[Loader[CV]]
}

trait SchemaLoaderProvider {
  def schemaLoader(datasetInfo: DatasetInfo): SchemaLoader
}

trait TruncatorProvider {
  val truncator: Truncator
}

trait DatasetMapReaderProvider {
  val datasetMapReader: DatasetMapReader
}

trait DatasetMapWriterProvider {
  val datasetMapWriter: DatasetMapWriter
}

trait DatasetMutatorProvider { this: TypeUniverse =>
  val lowLevelDatabaseMutator: LowLevelDatabaseMutator[CV]
  val datasetMutator: DatasetMutator[CV]
}

trait DatasetReaderProvider { this: TypeUniverse =>
  val lowLevelDatabaseReader: LowLevelDatabaseReader[CV]
  val datasetReader: DatasetReader[CV]
}

trait GlobalLogPlaybackProvider {
  val globalLogPlayback: GlobalLogPlayback
}

trait SecondaryManifestProvider {
  val secondaryManifest: SecondaryManifest
}

trait SecondaryPlaybackManifestProvider {
  def secondaryPlaybackManifest(storeId: String): PlaybackManifest
}

trait PlaybackToSecondaryProvider { this: TypeUniverse =>
  val playbackToSecondary: PlaybackToSecondary[CT, CV]
}

trait SecondaryConfigProvider {
  val secondaryConfig: SecondaryConfig
}

trait DatasetContentsCopierProvider {
  def datasetContentsCopier(datasetInfo: DatasetInfo): DatasetContentsCopier
}

trait GlobalLogProvider {
  val globalLog: GlobalLog
}
