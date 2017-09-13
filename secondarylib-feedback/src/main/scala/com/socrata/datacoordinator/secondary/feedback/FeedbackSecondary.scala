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

  type ComputationResult[RowHandle] = Either[ComputationFailure, Map[RowHandle, Map[UserColumnId, CV]]]

  /**
   * Perform the computation of the target column for each RowComputeInfo
   * @return The RowComputeInfo's and resulting values zipped with indexes of the rows
   * @note This should not throw any exception other than a [[ComputationFailure]] exception
   */
  def compute[RowHandle](sources: Map[RowHandle, Seq[PerCellData]]): ComputationResult[RowHandle]
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
   * Function to construct a DataCoordinatorClient for a given dataset and corresponding retry limit
   */
  def dataCoordinator: (String, CT => JValue => Option[CV]) => DataCoordinatorClient[CT,CV]
  val dataCoordinatorRetryLimit: Int

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

  private def checkRetriesLeft(cookie: FeedbackCookie, retriesLeft: Long, cause: String): Unit = {
    if (retriesLeft == 0) {
      val reason = s"Gave up replaying updates after too many failed $cause attempts"
      throw BrokenDatasetSecondaryException(reason, FeedbackCookie.encode(cookie.copyCurrent(errorMessage = Some(reason))))
    }
  }

  private def handleFeedbackResult[T](originalCookie: Option[FeedbackCookie],
                                      feedbackResult: FeedbackResult)(f: FeedbackCookie => T): T = {
    def encodeCookie(reason: String, f: FeedbackCookie => FeedbackCookie): Cookie = originalCookie match {
      case Some(fbCookie) => FeedbackCookie.encode(f(fbCookie))
      case None => Some(s"{errorMessage:$reason}")
    }

    feedbackResult match {
      case Success(updatedCookie) => f(updatedCookie)
      case ReplayLater(reason, transform) =>
        throw ReplayLaterSecondaryException(reason, encodeCookie(reason, transform))
      case BrokenDataset(reason, transform) =>
        throw BrokenDatasetSecondaryException(reason, encodeCookie(reason, transform))
      case FeedbackError(reason, cause) =>
        throw new Exception(reason, cause) // this will by caught by SW retry logic and version will be retried
    }
  }

  /** Provide the current copy an update.  The secondary should ignore it if it
    * already has this dataVersion.
    * @return a new cookie to store in the secondary map
    */
  override def version(datasetInfo: DatasetInfo, dataVersion: Long, cookie: Cookie, events: Iterator[Event[CT,CV]]): Cookie = {
    val datasetInternalName = datasetInfo.internalName

    val oldCookie = FeedbackCookie.decode(cookie).getOrElse {
      if (dataVersion == 1) {
        log.info("No existing cookie for dataset {} since it is version 1; creating cookie.")
        val schema = CookieSchema(
          dataVersion = DataVersion(0),
          copyNumber = CopyNumber(0),
          primaryKey = new UserColumnId(":id"), // this _should_ be right... but we will override this later
          columnIdMap = Map.empty,
          strategyMap = Map.empty,
          obfuscationKey = datasetInfo.obfuscationKey,
          computationRetriesLeft = computationRetryLimit,
          dataCoordinatorRetriesLeft = dataCoordinatorRetryLimit,
          resync = false
        )
        FeedbackCookie(current = schema, previous = None)
      } else {
        log.info("No existing cookie for dataset {}; going to resync.", datasetInternalName)
        throw ResyncSecondaryException(s"No cookie value for dataset $datasetInternalName")
      }
    }

    if (oldCookie.current.resync) {
      log.info("Cookie for dataset {} for version {} specified to resync.", datasetInternalName, dataVersion)
      throw ResyncSecondaryException("My cookie specified to resync!")
    }

    // mark the dataset as broken if we have run out of retries
    checkRetriesLeft(oldCookie, oldCookie.current.computationRetriesLeft, "computation")
    checkRetriesLeft(oldCookie, oldCookie.current.dataCoordinatorRetriesLeft, "data-coordinator")

    val expectedDataVersion = oldCookie.current.dataVersion.underlying + 1
    if (dataVersion < expectedDataVersion) {
      // if the data version is less than are equal to the version in our cookie, we have seen this version and are done
      log.info("Cookie for dataset {} expects data version {}, we have already replicated {}",
        datasetInternalName, expectedDataVersion.toString, dataVersion.toString)
      cookie
    } else if (dataVersion > expectedDataVersion) {
      // if the data version is more than 1 greater than the version in our cookie, this is unexpected and we should resync
      log.warn("Cookie for dataset {} expects data version {}, version {} is unexpected; going to resync.",
        datasetInternalName, expectedDataVersion.toString, dataVersion.toString)
      val reason = s"Unexpected data version $dataVersion"
      throw ResyncSecondaryException(reason)
    } else {
      // if the data version is 1 greater than the version in our cookie, we should try replaying it
      // on success we will return a cookie reflecting this version
      // on failure we will return the old cookie with retries decremented

      def handle[T](feedbackResult: FeedbackResult)(f: FeedbackCookie => T): T = {
        handleFeedbackResult(Some(oldCookie), feedbackResult)(f)
      }

      def notFirstEvent(event: String) =
        s"$event not the first event in version $dataVersion for dataset $datasetInternalName? Something is wrong!"

      def logErrorAndResync(message: String): Nothing = {
        log.error(message + " Going to resync.")
        throw ResyncSecondaryException(message)
      }

      val toJValueFunc = toJValue(datasetInfo.obfuscationKey)
      val fromJValueFunc = fromJValue(datasetInfo.obfuscationKey)
      val currentContext = datasetContext(datasetInternalName, toJValueFunc, fromJValueFunc)

      val cookieSeed = oldCookie.copy(oldCookie.current.copy(DataVersion(dataVersion)), oldCookie.previous)

      // WARNING: this .foldLeft(.) has side effects!
      case class FP(cookie: FeedbackCookie, columns: Set[UserColumnId])
      val resultFP = events.foldLeft(FP(cookieSeed, Set.empty)) { case (FP(newCookie, newCompCols), event) =>
        event match {
          case ColumnCreated(columnInfo) =>
            // note: we will see ColumnCreated events both when new columns are created and after a WorkingCopyCreated event

            // so... since we are blindly setting the primary key on version 1 to the expected value of ":id"
            // let's change that value to the UserColumnId of the column in the version that is said to actually
            // be the system primary key to not make any assumptions about what we actually name our system fields
            val newPrimaryKey =
              if (dataVersion == 1 && columnInfo.isSystemPrimaryKey) columnInfo.id
              else newCookie.current.primaryKey

            val old = newCookie.current.strategyMap
            val (updatedStrategyMap, updatedCompCols) = columnInfo.computationStrategyInfo match {
              case Some(strategy) if computationHandlers.exists(_.matchesStrategyType(strategy.strategyType)) && !old.contains(columnInfo.id) =>
                // only track computed columns as new if we haven't seen them before
                (old + (columnInfo.id -> strategy), newCompCols + columnInfo.id)
              case _ => (old, newCompCols)
            }
            FP(newCookie.copy(
              current = newCookie.current.copy(
                primaryKey = newPrimaryKey,
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
            // Should _always_ be the first event in a version
            if (newCompCols.nonEmpty) logErrorAndResync(notFirstEvent("WorkingCopyCreated"))
            val previous = if (dataVersion == 1) None else Some(oldCookie.current) // let's drop our fake version 0 cookie
            FP(newCookie.copy(
              current = newCookie.current.copy(
                copyNumber = CopyNumber(copyInfo.copyNumber)
              ),
              previous = previous
            ), newCompCols)
          case WorkingCopyDropped =>
            // Should be the first event in this version
            if (newCompCols.nonEmpty) logErrorAndResync(notFirstEvent("WorkingCopyDropped"))
            FP(newCookie.copy(
              current = newCookie.previous.getOrElse {
                log.info("No previous value in cookie for dataset {}. Going to resync.", datasetInternalName)
                throw ResyncSecondaryException(s"No previous value in cookie for dataset $datasetInternalName")
              }.copy(dataVersion = DataVersion(dataVersion)),
              previous = None
            ), newCompCols)
          case RowDataUpdated(operations) =>
            // in practice this should only happen once per data version
            // flush handling of newly created computed columns
            handle(currentContext(newCookie).flushColumnCreations(newCompCols)) { updatedCookie =>
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
              handle(currentContext(updatedCookie).feedback(toCompute))(FP(_, Set.empty))
            }
          case _ => // no-ops for us
            // flush handling of newly created computed columns
            handle(currentContext(newCookie).flushColumnCreations(newCompCols))(FP(_, Set.empty))
        }
      }
      handle(currentContext(resultFP.cookie).flushColumnCreations(resultFP.columns))(FeedbackCookie.encode)
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
          val result = datasetContext(
            datasetInfo.internalName,
            toJValueFunc,
            fromJValueFunc)(newCookie).feedback(rws.map { row => Row(row, None) }, resync = true)
          handleFeedbackResult(originalCookie = None, feedbackResult =  result) { resultCookie =>
            newCookie = resultCookie
          }
        }
      }

      FeedbackCookie.encode(newCookie)
    } catch {
      case replayLater: ReplayLaterSecondaryException =>
        throw replayLater
      case brokenDataset: BrokenDatasetSecondaryException =>
        throw brokenDataset
      case error: Exception =>
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
 * Failure type to be returned by FeedbackSecondary.compute(.)
 */
sealed abstract class ComputationFailure {
  def reason: String
  def cause: Option[Exception]
}

case class ComputationError(reason: String, cause: Option[Exception] = None) extends ComputationFailure
case class FatalComputationError(reason: String, cause: Option[Exception] = None) extends ComputationFailure
