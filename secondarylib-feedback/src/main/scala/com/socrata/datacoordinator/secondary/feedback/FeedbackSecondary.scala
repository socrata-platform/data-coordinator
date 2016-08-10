package com.socrata.datacoordinator.secondary.feedback

import java.io.IOException

import com.rojoma.json.v3.ast._
import com.rojoma.simplearm.Managed
import com.socrata.datacoordinator.id.{UserColumnId, StrategyType}
import com.socrata.datacoordinator.secondary
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.secondary.feedback.monitor.StatusMonitor
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.http.client.HttpClient

trait HasStrategy {
  def strategy: ComputationStrategyInfo
}

trait ComputationHandler[CT,CV] {
  type PerDatasetData
  type PerColumnData <: HasStrategy
  type PerCellData

  /**
   * A FeedbackSecondary operates on computed columns of certain strategy types.
   * @return Returns true if `typ` matches a strategy type of this
   */
  def matchesStrategyType(typ: StrategyType): Boolean

  /** Extract from the `strategy` and `cookie` any dataset-global information required
    * for further processing.
    */
  def setupDataset(cookie: CookieSchema): PerDatasetData

  /** Set up information required on a per-column basis. */
  def setupColumn(dataset: PerDatasetData, strategy: ComputationStrategyInfo, targetColId: UserColumnId): PerColumnData

  /**
   * @return The PerCellData for performing the computation of the target column
   */
  def setupCell(column: PerColumnData, row: secondary.Row[CV]): PerCellData

  /**
   * Perform the computation of the target column for each RowComputeInfo
   * @return The RowComputeInfo's and resulting values zipped with indexes of the rows
   * @note This should not throw any exception other than a [[ComputationFailure]] exception
   */
  def compute[RowHandle](sources: Map[RowHandle, Seq[PerCellData]]): Map[RowHandle, Map[UserColumnId, CV]]
}

/**
 * A FeedbackSecondary is a secondary that processes updates to source columns of computed columns
 * and "feeds back" those updates to data-coordinator via posting mutation scripts.
 */
abstract class FeedbackSecondary[CT,CV] extends Secondary[CT,CV] {

  val log = org.slf4j.LoggerFactory.getLogger(classOf[FeedbackSecondary[CT,CV]])

  /**
   * The `user` to specify in mutation scripts
   */
  val user: String

  /**
   * The batch size of mutation scripts and general processing; should be a function of `width` of dataset.
   */
  val baseBatchSize: Int

  def batchSize(width: Int): Int = baseBatchSize // TODO: figure out what this should be

  /**
   * The status monitor to report upon completion of batches for datasets
   */
  val statusMonitor: StatusMonitor

  /**
   * HTTP connection to data-coordinator and corresponding retry limits
   */
  val httpClient: HttpClient

  def hostAndPort(instanceName: String): Option[(String, Int)]

  val dataCoordinatorRetryLimit: Int
  val internalDataCoordinatorRetryLimit: Int

  /**
   * Computation handlers and corresponding retry limit
   */
  val computationHandlers: Seq[ComputationHandler[CT,CV]]
  val computationRetryLimit: Int

  /**
   * Functions for converting between json and column types and values
   */
  val repFor: Array[Byte] => CT => CV => JValue
  val repFrom: Array[Byte] => CT => JValue => Option[CV]
  val typeFor: CV => Option[CT]
  val typeFromJValue: JValue => Option[CT]

  private def toJValue(obfuscationKey: Array[Byte]): CV => JValue = {
    val reps = repFor(obfuscationKey);
    { value: CV =>
      typeFor(value) match {
        case Some(typ) => reps(typ)(value)
        case None => JNull
      }
    }
  }

  private def fromJValue(obfuscationKey: Array[Byte]): CT => JValue => Option[CV] = {
    val reps = repFrom(obfuscationKey);
    reps
  }

  // this is a lazy val because we want abstract `httpClient` to be initialized first
  private lazy val dataCoordinator = {
    assert(httpClient != null)
    DataCoordinatorClient[CT,CV](httpClient, hostAndPort, internalDataCoordinatorRetryLimit, typeFromJValue)
  }

  private def datasetContext(datasetInternalName: String,
                             toJValueFunc: CV => JValue,
                             fromJValueFunc: CT => JValue => Option[CV]): FeedbackCookie => FeedbackContext[CT,CV] =
    FeedbackContext(
      user,
      batchSize,
      statusMonitor,
      computationHandlers,
      computationRetryLimit,
      dataCoordinator,
      dataCoordinatorRetryLimit,
      datasetContext = (datasetInternalName, toJValueFunc, fromJValueFunc)
    )

  private val systemId = new UserColumnId(":id")

  /** The dataset has been deleted. */
  override def dropDataset(datasetInternalName: String, cookie: Cookie): Unit = {} // nothing to do here

  /**
   * @return The `dataVersion` of the latest copy this secondary has.  Should
   *         return 0 if this ID does not name a known dataset.
   */
  override def currentVersion(datasetInternalName: String, cookie: Cookie): Long = {
    FeedbackCookie.decode(cookie) match {
      case Some(ck) => ck.current.dataVersion.underlying
      case None => log.debug("No existing cookie for dataset {}", datasetInternalName); 0
    }
  }

  /**
   * @return The `copyNumber` of the latest copy this secondary has.  Should
   *         return 0 if this ID does not name a known dataset.
   */
  override def currentCopyNumber(datasetInternalName: String, cookie: Cookie): Long = {
    FeedbackCookie.decode(cookie) match {
      case Some(ck) => ck.current.copyNumber.underlying
      case None => log.debug("No existing cookie for dataset {}", datasetInternalName); 0
    }
  }

  /** Provide the current copy an update.  The secondary should ignore it if it
    * already has this dataVersion.
    * @return a new cookie to store in the secondary map
    */
  override def version(datasetInfo: DatasetInfo, dataVersion: Long, cookie: Cookie, events: Iterator[Event[CT,CV]]): Cookie = {
    try {
      val datasetInternalName = datasetInfo.internalName

      val oldCookie = FeedbackCookie.decode(cookie).getOrElse {
        log.info("No existing cookie for dataset {}; going to resync.", datasetInternalName)
        throw ResyncSecondaryException(s"No cookie value for dataset $datasetInternalName")
      }

      if (oldCookie.current.resync) {
        log.info("Cookie for dataset {} for version {} specified to resync.", datasetInternalName, dataVersion)
        throw ResyncSecondaryException("My cookie specified to resync!")
      }

      var result = cookie
      if (dataVersion != oldCookie.current.dataVersion.underlying
        || (oldCookie.current.computationRetriesLeft > 0 && oldCookie.current.dataCoordinatorRetriesLeft > 0)) {
        // either we are not up to date on the data version or we still have retries left on the current version
        // note: we set the retries left to 0 once we have succeeded for the current version
        val toJValueFunc = toJValue(datasetInfo.obfuscationKey)
        val fromJValueFunc = fromJValue(datasetInfo.obfuscationKey)
        val currentContext = datasetContext(datasetInternalName, toJValueFunc, fromJValueFunc)

        val cookieSeed = oldCookie.copy(oldCookie.current.copy(DataVersion(dataVersion)), oldCookie.previous)

        // WARNING: this .foldLeft(.) has side effects!
        case class FP(cookie: FeedbackCookie, columns: Set[UserColumnId])
        val resultFP = events.foldLeft(FP(cookieSeed, Set.empty)) { case (FP(newCookie, newCompCols), event) =>
          event match {
            case ColumnCreated(columnInfo) =>
              val old = newCookie.current.strategyMap
              val (updatedStrategyMap, updatedCompCols) = columnInfo.computationStrategyInfo match {
                case Some(strategy) if computationHandlers.exists(_.matchesStrategyType(strategy.strategyType)) =>
                  (old + (columnInfo.id -> strategy), newCompCols + columnInfo.id)
                case _ => (old, newCompCols)
              }
              FP(newCookie.copy(
                current = newCookie.current.copy(
                  columnIdMap = newCookie.current.columnIdMap + (columnInfo.id -> columnInfo.systemId),
                  strategyMap = updatedStrategyMap
                )
              ), updatedCompCols)
            case ColumnRemoved(columnInfo) =>
              val old = newCookie.current.strategyMap
              val (updatedStrategyMap, updatedCompCols) = columnInfo.computationStrategyInfo match {
                case Some(strategy) if computationHandlers.exists(_.matchesStrategyType(strategy.strategyType)) =>
                  (old - columnInfo.id, newCompCols - columnInfo.id)
                case _ => (old, newCompCols)
              }
              FP(newCookie.copy(
                current = newCookie.current.copy(
                  columnIdMap = newCookie.current.columnIdMap - columnInfo.id,
                  strategyMap = updatedStrategyMap
                )
              ), updatedCompCols)
            case RowIdentifierSet(columnInfo) =>
              FP(newCookie.copy(
                current = newCookie.current.copy(
                  primaryKey = columnInfo.id
                )
              ), newCompCols)
            case RowIdentifierCleared(columnInfo) =>
              FP(newCookie.copy(
                current = newCookie.current.copy(
                  primaryKey = systemId
                )
              ), newCompCols)
            case SystemRowIdentifierChanged(columnInfo) =>
              FP(newCookie.copy(
                current = newCookie.current.copy(
                  primaryKey = columnInfo.id
                )
              ), newCompCols)
            case WorkingCopyCreated(copyInfo) =>
              // Should be the first event in this version
              if (newCompCols.nonEmpty) logErrorAndResync(notFirstEvent("WorkingCopyCreated"))
              FP(newCookie.copy(
                current = newCookie.current.copy(
                  copyNumber = CopyNumber(copyInfo.copyNumber)
                ),
                previous = Some(newCookie.current)
              ), newCompCols)
            case WorkingCopyDropped =>
              // Should be the first event in this version
              if (newCompCols.nonEmpty) logErrorAndResync(notFirstEvent("WorkingCopyCreated"))
              FP(newCookie.copy(
                current = newCookie.previous.getOrElse {
                  log.info("No previous value in cookie for dataset {}. Going to resync.", datasetInternalName)
                  throw ResyncSecondaryException(s"No previous value in cookie for dataset $datasetInternalName")
                },
                previous = None
              ), newCompCols)
            case RowDataUpdated(operations) =>
              // in practice this should only happen once per data version
              // flush handling of newly created computed columns
              val updatedCookie =
                currentContext(newCookie).flushColumnCreations(newCompCols)

              val toCompute = operations.toIterator.map { case op =>
                op match {
                  case insert: Insert[CV] =>
                    Some(Row(insert.data, None))
                  case update: Update[CV] =>
                    Some(Row(update.data, update.oldData))
                  case delete: Delete[CV] => // no-op
                    None
                }
              }.filter(x => x.isDefined).map(x => x.get)

              log.info("Processing row update of dataset {} in version {}...", datasetInternalName, dataVersion)
              FP(currentContext(updatedCookie).feedback(toCompute), Set.empty)
            case _ => // no-ops for us
              // flush handling of newly created computed columns
              val updatedCookie =
                currentContext(newCookie).flushColumnCreations(newCompCols)
              FP(updatedCookie, Set.empty)
          }
        }
        val resultCookie =
          currentContext(resultFP.cookie).flushColumnCreations(resultFP.columns)
        result = FeedbackCookie.encode(resultCookie)
      }

      def notFirstEvent(event: String) =
        s"$event not the first event in version $dataVersion for dataset $datasetInternalName? Something is wrong!"

      def logErrorAndResync(message: String): Nothing = {
        log.error(message + " Going to resync.")
        throw ResyncSecondaryException(message)
      }

      result
    } catch {
      case resyncException: ResyncSecondaryException =>
        throw resyncException
      case replayLater: ReplayLaterSecondaryException =>
        throw replayLater
      case brokenDataset: BrokenDatasetSecondaryException =>
        throw brokenDataset
      case error: Throwable =>
        log.error(s"An unexpected error has occurred: ${error.getMessage}\n{}", error.getStackTrace.toString)
        throw error
    }
  }

  /**
   * Part of the resync path for (un)published copies.
   */
  override def resync(datasetInfo: DatasetInfo, copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo[CT]], cookie: Cookie,
                      rows: Managed[Iterator[ColumnIdMap[CV]]], rollups: Seq[RollupInfo], isLatestLivingCopy: Boolean): Cookie = {
    try {
      // update cookie
      val copyNumber = CopyNumber(copyInfo.copyNumber)

      val primaryKey = schema.filter {
        case (colId, colInfo) => colInfo.isUserPrimaryKey
      }.toSeq.map({ case (_, colInfo) => colInfo.id}).headOption.getOrElse(systemId)

      val columnIdMap = schema.toSeq.map { case (colId, colInfo) => (colInfo.id, colId) }.toMap

      val strategyMap = schema.toSeq.filter {
        case (_, colInfo) => colInfo.computationStrategyInfo.exists(cs => computationHandlers.exists(_.matchesStrategyType(cs.strategyType)))
      }.map { case (_, colInfo) => (colInfo.id, colInfo.computationStrategyInfo.get) }.toMap

      val cookieSchema = CookieSchema(
        dataVersion = DataVersion(copyInfo.dataVersion),
        copyNumber = copyNumber,
        primaryKey = primaryKey,
        columnIdMap = columnIdMap,
        strategyMap = strategyMap,
        computationRetriesLeft = computationRetryLimit,
        dataCoordinatorRetriesLeft = dataCoordinatorRetryLimit,
        obfuscationKey = datasetInfo.obfuscationKey,
        resync = false
      )

      var newCookie = FeedbackCookie(cookieSchema, None)

      val toJValueFunc = toJValue(datasetInfo.obfuscationKey)
      val fromJValueFunc = fromJValue(datasetInfo.obfuscationKey)
      if (isLatestLivingCopy && newCookie.current.strategyMap.nonEmpty) {
        for {
          rws <- rows
        } {
          newCookie = datasetContext(
            datasetInfo.internalName,
            toJValueFunc,
            fromJValueFunc
          )(newCookie).feedback(rws.map { row => Row(row, None) }, resync = true)
        }
      }

      FeedbackCookie.encode(newCookie)
    } catch {
      case replayLater: ReplayLaterSecondaryException =>
        throw replayLater
      case brokenDataset: BrokenDatasetSecondaryException =>
        throw brokenDataset
      case error: Throwable =>
        // note: shouldn't ever throw a ResyncSecondaryException from inside resync
        log.error(s"An unexpected error has occurred: ${error.getMessage}\n{}", error.getStackTrace.toString)
        throw error
    }
  }

  /**
   * Part of the resync path for discarded/snapshotted copies.
   */
  // no-op: feedback secondaries don't need to feedback values for discarded/snapshotted copies
  override def dropCopy(datasetInfo: DatasetInfo, copyInfo: CopyInfo, cookie: Cookie, isLatestCopy: Boolean): Cookie = cookie
}

/**
 * Exception type to be thrown by FeedbackSecondary.compute(.)
 */
case class ComputationFailure(reason: String, cause: Throwable) extends Exception(reason, cause)

object ComputationFailure {

  def apply(reason: String): ComputationFailure = ComputationFailure(reason, null)
}
