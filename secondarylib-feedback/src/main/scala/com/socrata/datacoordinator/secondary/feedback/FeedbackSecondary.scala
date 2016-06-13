package com.socrata.datacoordinator.secondary.feedback

import java.io.IOException

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import com.rojoma.json.v3.io.{JValueEventIterator, JsonReaderException}
import com.rojoma.json.v3.util.{NoTag, SimpleHierarchyCodecBuilder, JsonUtil, AutomaticJsonCodecBuilder}
import com.rojoma.simplearm.Managed
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId, StrategyType}
import com.socrata.datacoordinator.secondary
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.secondary.feedback.monitor.StatusMonitor
import com.socrata.datacoordinator.util.collection.{MutableColumnIdMap, ColumnIdMap}
import com.socrata.http.client.{RequestBuilder, HttpClient}
import com.socrata.http.client.exceptions.{ContentTypeException, HttpClientException}

import scala.collection.SortedSet

trait ComputationHandler[CT,CV] {
  type RCI <: RowComputeInfo[CV]

  /**
   * @return The `user` to specify in mutation scripts
   */
  def user: String

  /**
   * @return The number of times to retry the computation step via replaying later.
   */
  def computationRetries: Int

  /**
   * A FeedbackSecondary operates on computed columns of certain strategy types.
   * @return Returns true if `typ` matches a strategy type of this
   */
  def matchesStrategyType(typ: StrategyType): Boolean

  /**
   * @return The RowComputeInfo for performing the computation of the target column
   */
  def transform(row: secondary.Row[CV], targetColId: UserColumnId, strategy: ComputationStrategyInfo, cookie: CookieSchema): RCI

  /**
   * Perform the computation of the target column for each RowComputeInfo
   * @return The RowComputeInfo's and resulting values zipped with indexes of the rows
   * @note This should not throw any exception other than a [[ComputationFailure]] exception
   */
  def compute[RowHandle](sources: Map[RowHandle, Seq[RCI]]): Map[RowHandle, Map[UserColumnId, CV]]

}

/**
 * A FeedbackSecondary is a secondary that processes updates to source columns of computed columns
 * and "feeds back" those updates to data-coordinator via posting mutation scripts.
 */
abstract class FeedbackSecondary[CT,CV] extends Secondary[CT,CV] {

  val log = org.slf4j.LoggerFactory.getLogger(classOf[FeedbackSecondary[CT,CV]])

  val httpClient: HttpClient
  def hostAndPort(instanceName: String): Option[(String, Int)]
  val baseBatchSize: Int
  val internalMutationScriptRetries: Int
  val mutationScriptRetries: Int
  val computationHandlers: Seq[ComputationHandler[CT, CV]]
  val repFor: Array[Byte] => CT => CV => JValue
  val typeFor : CV => Option[CT]
  val statusMonitor: StatusMonitor

  private val systemId = new UserColumnId(":id")

  /**
   * @return The batch size of mutation scripts and general processing; should be a function of `width` of dataset.
   */
  def batchSize(width: Int): Int = 10000 // TODO: figure out what this should be

  def toJValue(obfuscationKey: Array[Byte]): CV => JValue = {
    val reps = repFor(obfuscationKey);
    { value: CV =>
     typeFor(value) match {
       case Some(typ) => reps(typ)(value)
       case None => JNull
     }
    }
  }

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
      val oldCookie = FeedbackCookie.decode(cookie).getOrElse {
        log.info("No existing cookie for dataset {}; going to resync.", datasetInfo.internalName)
        throw ResyncSecondaryException(s"No cookie value for dataset ${datasetInfo.internalName}")
      }

      if (oldCookie.current.resync) {
        log.info("Cookie for dataset {} for version {} specified to resync.", datasetInfo.internalName, dataVersion)
        throw ResyncSecondaryException("My cookie specified to resync!")
      }

      var result = cookie
      if (dataVersion != oldCookie.current.dataVersion.underlying
        || (oldCookie.current.computationRetriesLeft > 0 && oldCookie.current.mutationScriptRetriesLeft > 0)) {
        // either we are not up to date on the data version or we still have retries left on the current version
        // note: we set the retries left to 0 once we have succeeded for the current version
        val toJValueFunc = toJValue(datasetInfo.obfuscationKey)
        var newCookie = oldCookie.copy(oldCookie.current.copy(DataVersion(dataVersion)), oldCookie.previous)

        val emptyIter = Iterator[Row]()
        val toCompute = events.collect { case event =>
          event match {
            case ColumnCreated(columnInfo) =>
              val old = newCookie.current.strategyMap
              val updated = columnInfo.computationStrategyInfo match {
                case Some(strategy) =>
                  if (computationHandlers.exists(_.matchesStrategyType(strategy.strategyType))) old + (columnInfo.id -> strategy) else old
                case None => old
              }
              newCookie = newCookie.copy(
                current = newCookie.current.copy(
                  columnIdMap = newCookie.current.columnIdMap + (columnInfo.id -> columnInfo.systemId),
                  strategyMap = updated
                )
              )
              emptyIter
            case ColumnRemoved(columnInfo) =>
              val old = newCookie.current.strategyMap
              val updated = columnInfo.computationStrategyInfo match {
                case Some(strategy) =>
                  if (computationHandlers.exists(_.matchesStrategyType(strategy.strategyType))) old - columnInfo.id else old
                case None => old
              }
              newCookie = newCookie.copy(
                current = newCookie.current.copy(
                  columnIdMap = newCookie.current.columnIdMap - columnInfo.id,
                  strategyMap = updated
                )
              )
              emptyIter
            case RowIdentifierSet(columnInfo) =>
              newCookie = newCookie.copy(
                current = newCookie.current.copy(
                  primaryKey = columnInfo.id
                )
              )
              emptyIter
            case RowIdentifierCleared(columnInfo) =>
              newCookie = newCookie.copy(
                current = newCookie.current.copy(
                  primaryKey = systemId
                )
              )
              emptyIter
            case SystemRowIdentifierChanged(columnInfo) =>
              newCookie = newCookie.copy(
                current = newCookie.current.copy(
                  primaryKey = columnInfo.id
                )
              )
              emptyIter
            case WorkingCopyCreated(copyInfo) =>
              newCookie = newCookie.copy(
                current = newCookie.current.copy(
                  copyNumber = CopyNumber(copyInfo.copyNumber)
                ),
                previous = Some(newCookie.current)
              )
              emptyIter
            case WorkingCopyDropped =>
              newCookie = newCookie.copy(
                current = newCookie.previous.getOrElse {
                  log.warn("No previous value in cookie for dataset {}. Going to resync.", datasetInfo.internalName)
                  throw ResyncSecondaryException(s"No previous value in cookie for dataset ${datasetInfo.internalName}")
                },
                previous = None
              )
              emptyIter
            case RowDataUpdated(operations) =>
              // no cookie update needed here

              operations.toIterator.map { case op =>
                op match {
                  case insert: Insert[CV] =>
                    Some(Row(insert.data, None))
                  case update: Update[CV] =>
                    Some(Row(update.data, update.oldData))
                  case delete: Delete[CV] => // no-op
                    None
                }
              }.filter(x => x.isDefined).map(x => x.get)

            case _ => // no-ops for us
              emptyIter
          }
        }.flatten

        val context = new LocalContext(newCookie, toJValueFunc)
        newCookie = context.feedback(datasetInfo.internalName, toCompute)
        result = FeedbackCookie.encode(newCookie)
      }
      result
    } catch {
      case resyncException: ResyncSecondaryException =>
        throw resyncException
      case replayLater: ReplayLaterSecondaryException =>
        throw replayLater
      case brokenDataset: BrokenDatasetSecondaryException =>
        throw brokenDataset
      case error : Throwable =>
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
      // decode the old cookie
      val oldCookie = FeedbackCookie.decode(cookie)

      // update cookie
      val copyNumber = CopyNumber(copyInfo.copyNumber)

      val primaryKey = schema.filter {
        case (colId, colInfo) => colInfo.isUserPrimaryKey
      }.toSeq.map({ case (_, colInfo) => colInfo.id}).headOption.getOrElse(systemId)

      val columnIdMap = schema.toSeq.map { case (colId, colInfo) => (colInfo.id, colId) }.toMap

      val strategyMap = schema.toSeq.filter {
        case (_, colInfo) => colInfo.computationStrategyInfo.exists(cs => computationHandlers.exists(_.matchesStrategyType(cs.strategyType)))
      }.map { case (_, colInfo) => (colInfo.id, colInfo.computationStrategyInfo.get)} .toMap

      val extra = oldCookie.map(_.current.extra).getOrElse(JNull)

      val previous = oldCookie.map(_.current)

      val cookieSchema = CookieSchema(
        dataVersion = DataVersion(copyInfo.dataVersion),
        copyNumber = copyNumber,
        primaryKey = primaryKey,
        columnIdMap = columnIdMap,
        strategyMap = strategyMap,
        computationRetriesLeft = computationHandlers.map(_.computationRetries).max,
        mutationScriptRetriesLeft = mutationScriptRetries,
        obfuscationKey = datasetInfo.obfuscationKey,
        resync = false,
        extra = extra
      )

      var newCookie = FeedbackCookie(cookieSchema, previous)

      val toJValueFunc = toJValue(datasetInfo.obfuscationKey)
      if (isLatestLivingCopy && newCookie.current.strategyMap.nonEmpty) {
        for {
          rws <- rows
        } {
          val context = new LocalContext(newCookie, toJValueFunc)
          newCookie = context.feedback(datasetInfo.internalName, rws.map(row => Row(row, None)), resync = true)
        }
      }

      FeedbackCookie.encode(newCookie)
    } catch {
      case replayLater: ReplayLaterSecondaryException =>
        throw replayLater
      case brokenDataset: BrokenDatasetSecondaryException =>
        throw brokenDataset
      case error : Throwable =>
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

  case class Row(data: secondary.Row[CV], oldData: Option[secondary.Row[CV]])

  private class LocalContext(feedbackCookie: FeedbackCookie, toJValueFunc: CV => JValue) {

    val cookie = feedbackCookie.current

    def mergeWith[A, B](xs: Map[A, B], ys: Map[A, B])(f: (B, B) => B): Map[A, B] = {
      ys.foldLeft(xs) { (combined, yab) =>
        val (a,yb) = yab
        val newB = combined.get(a) match {
          case None => yb
          case Some(xb) => f(xb, yb)
        }
        combined.updated(a, newB)
      }
    }

    // this may throw a ResyncSecondaryException, a ReplayLaterSecondaryException, or a BrokenDatasetSecondaryException
    def feedback(datasetInternalName: String, rows: Iterator[Row], resync: Boolean = false): FeedbackCookie = {
      val width = cookie.columnIdMap.size
      val size = batchSize(width)

      log.info("Processing update of dataset {} to version {} with batch_size = {}",
        datasetInternalName, cookie.dataVersion.underlying.toString, size.toString)

      def b(retriesLeft: Int, message: String): Int = {
        if (retriesLeft == 0) throw new BrokenDatasetSecondaryException(message)
        retriesLeft
      }

      var count = 0
      try {
        rows.grouped(size).foreach { batchSeq =>
          val batch: Seq[Row] = batchSeq.toIndexedSeq
          count += 1
          val updates = computationHandlers.foldLeft(Map.empty[Int, Map[UserColumnId, CV]]) { (currentUpdates, computationHandler) =>
            val currentRows = batch.toArray
            for((idx, upds) <- currentUpdates) {
              val currentRow = MutableColumnIdMap(currentRows(idx).data)
              for((cid, cv) <- upds) {
                currentRow(cookie.columnIdMap(cid)) = cv
              }
              currentRows(idx) = currentRows(idx).copy(data = currentRow.freeze())
            }
            val newUpdates = computeUpdates(computationHandler, currentRows)
            mergeWith(currentUpdates, newUpdates)(_ ++ _)
          }
          val jsonUpdates = updates.valuesIterator.map { updates =>
            JsonEncode.toJValue(updates.mapValues(toJValueFunc))
          }
          writeMutationScript(jsonUpdates) match {
            case Some(script) =>
              val result = postMutationScript(datasetInternalName, script)
              result match {
                case Success =>
                  log.info("Finished batch {} of approx. {} rows", count, size)
                  statusMonitor.update(datasetInternalName, cookie.dataVersion, size, count)
                case ftddc@FailedToDiscoverDataCoordinator =>
                  log.warn("Failed to discover data-coordinator; going to request to have this dataset replayed later")
                  // we will retry this "indefinitely"; do not decrement retries left
                  throw ReplayLaterSecondaryException(ftddc.english, FeedbackCookie.encode(feedbackCookie.copy(current = cookie.copy(resync = resync))))
                case ddb@DataCoordinatorBusy =>
                  log.info("Received a 409 from data-coordinator; going to request to have this dataset replayed later")
                  // we will retry this "indefinitely"; do not decrement retries left
                  throw ReplayLaterSecondaryException(ddb.english, FeedbackCookie.encode(feedbackCookie.copy(current = cookie.copy(resync = resync))))
                case ddne@DatasetDoesNotExist =>
                  log.info("Completed updating dataset {} to version {} early: {}", datasetInternalName, cookie.dataVersion.underlying.toString, ddne.english)
                  statusMonitor.remove(datasetInternalName, cookie.dataVersion, count)

                  // nothing more to do here
                  return feedbackCookie.copy(
                    current = cookie.copy(
                      computationRetriesLeft = 0,
                      mutationScriptRetriesLeft = 0,
                      resync = false
                    )
                  )
                case schemaChange =>
                  log.info("A schema change occurred breaking my upsert: {}; going to start a resync...", schemaChange.english)
                  throw ResyncSecondaryException(s"A schema change occurred breaking my upsert: ${schemaChange.english}")
              }
            case None =>
              log.info("Batch {} had no rows to update of approx. {} rows; no script posted.", count, size)
              statusMonitor.update(datasetInternalName, cookie.dataVersion, size, count)
          }
        }
      } catch {
        case ComputationFailure(reason, cause) =>
          // Some failure has occurred in computation; we will only retry these exceptions so many times
          val gaveUp = "Gave up replaying updates after too many failed computation attempts"
          val newCookie = feedbackCookie.copy(
            current = cookie.copy(
              computationRetriesLeft = b(cookie.computationRetriesLeft - 1, gaveUp), // decrement retries
              mutationScriptRetriesLeft = mutationScriptRetries, // reset mutation script retries
              resync = resync
            )
          )
          throw ReplayLaterSecondaryException(reason, FeedbackCookie.encode(newCookie))
        case FeedbackFailure(reason, cause) =>
          // We failed to post our mutation script to data-coordinator for some reason
          val gaveUp = "Gave up replaying updates after too many failed mutation script attempts"
          val newCookie = feedbackCookie.copy(
            current = cookie.copy(
              computationRetriesLeft = computationHandlers.map(_.computationRetries).max, // reset computation retries
              mutationScriptRetriesLeft = b(cookie.mutationScriptRetriesLeft - 1, gaveUp), // decrement retries
              resync = resync
            )
          )
          throw ReplayLaterSecondaryException(reason, FeedbackCookie.encode(newCookie))
      }
      log.info("Completed updating dataset {} to version {} after batch: {}",
        datasetInternalName, cookie.dataVersion.underlying.toString, count.toString)
      statusMonitor.remove(datasetInternalName, cookie.dataVersion, count)

      // success!
      feedbackCookie.copy(
        current = cookie.copy(
          computationRetriesLeft = 0,
          mutationScriptRetriesLeft = 0,
          resync = false
        )
      )
    }

    // this may throw a ComputationFailure exception
    private def computeUpdates(computationHandler: ComputationHandler[CT,CV], rows: IndexedSeq[Row]): Map[Int,Map[UserColumnId, CV]] = {
      val strategies = cookie.strategyMap.toSeq
      val toCompute = rows.iterator.zipWithIndex.flatMap { case (row, index) =>
        val rcis =
          strategies.flatMap { case (targetColId: UserColumnId, strategy) =>
            // don't compute if there has been to change to the source columns
            if (noChange(row, strategy.sourceColumnIds))
              None
            else
              Some(computationHandler.transform(row.data, targetColId, strategy, cookie))
          }
        if(rcis.isEmpty) Iterator.empty
        else Iterator.single(index -> rcis)
      }.toMap

      val results = computationHandler.compute(toCompute)

      results.flatMap { case (rowIdx, updates) =>
        val row = rows(rowIdx)
        val filtered = updates.filter { case (colId: UserColumnId, value) =>
          extractCV(row.data, colId) != Some(value) // don't update to current value
        }
        val rowId = extractCV(row.data, cookie.primaryKey) match {
          case None => throw new Exception(s"Cannot find value for primary key ${cookie.primaryKey} in row: $row!")
          case Some(other) => other
        }
        if (filtered.nonEmpty) {
          val newRow = Map(
            cookie.primaryKey -> rowId
          ) ++ filtered.map { case (id, value) => (id, value)}.toMap
          Some(rowIdx -> newRow)
        } else {
          None
        }
      }
    }

    private def noChange(row: Row, columns: Seq[UserColumnId]): Boolean = row.oldData match {
      case Some(old) =>
        columns.forall { id =>
          val internal = cookie.columnIdMap(id)
          row.data(internal) == old(internal) // safe because updates contain _all_ row values (and this must be one)
        }
      case None => false
    }

    private def extractCV(row: secondary.Row[CV], userId: UserColumnId): Option[CV] = {
      val internal = cookie.columnIdMap(userId)
      row.get(internal)
    }

    // commands for mutation scripts
    val commands = Seq(
      JObject(Map(
        "c" -> JString("normal"),
        "user" -> JString(computationHandlers.map(_.user).to[SortedSet].mkString(","))
      )),
      JObject(Map(
        "c" -> JString("row data"),
        "update_only" -> JBoolean.canonicalTrue,
        "nonfatal_row_errors" -> JArray(Array(JString("insert_in_update_only")))
      ))
    )

    private def writeMutationScript(rowUpdates: Iterator[JValue]): Option[JArray] = {
      if (!rowUpdates.hasNext) None
      else Some(JArray(commands ++ rowUpdates))
    }

    val UpdateDatasetDoesNotExist = "update.dataset.does-not-exist"
    val UpdateRowUnknownColumn = "update.row.unknown-column"
    val UpdateRowPrimaryKeyNonexistentOrNull = "update.row.primary-key-nonexistent-or-null"

    private def fail(message: String): Nothing = {
      log.error(message)
      throw FeedbackFailure(message)
    }

    private def fail(message: String, cause: Throwable): Nothing = {
      log.error(message)
      throw FeedbackFailure(message, cause)
    }

    private def retrying[T](actions: => T, remainingAttempts: Int = internalMutationScriptRetries): T = {
      val failure = try {
        return actions
      } catch {
        case e: IOException => e
        case e: HttpClientException => e
        case e: JsonReaderException => e
      }

      log.info("Failure occurred while posting mutation script: {}", failure.getMessage)
      if (remainingAttempts > 0) retrying(actions, remainingAttempts - 1)
      else fail(s"Ran out of retry attempts after failure: ${failure.getMessage}", failure)
    }

    sealed abstract class MutationScriptResult {
      def english: String
    }

    case object Success extends MutationScriptResult {
      val english = "Successfully upserted rows"
    }

    case object FailedToDiscoverDataCoordinator extends MutationScriptResult {
      val english = "Failed to discover data-coordinator host and port"
    }

    case object DataCoordinatorBusy extends MutationScriptResult {
      val english = "Data-coordinator responded with 409"
    }

    case object DatasetDoesNotExist extends MutationScriptResult {
      val english = "Dataset does not exist"
    }

    case class PrimaryKeyColumnDoesNotExist(id: UserColumnId) extends MutationScriptResult {
      val english = s"Primary key column ${id.underlying} does not exist"
    }

    case class TargetColumnDoesNotExist(id: UserColumnId) extends MutationScriptResult {
      val english = s"Target column ${id.underlying} does not exist"
    }

    case object PrimaryKeyColumnHasChanged extends MutationScriptResult {
      val english = "The primary key column has changed"
    }

    case class ErrorResponse(errorCode: String, data: JObject)

    implicit val erCodec = AutomaticJsonCodecBuilder[ErrorResponse]

    sealed class Response

    case class Upsert(typ: String, id: String, ver: String) extends Response

    case class NonfatalError(typ: String, err: String, id: Option[String]) extends Response

    implicit val upCodec = AutomaticJsonCodecBuilder[Upsert]
    implicit val neCodec = AutomaticJsonCodecBuilder[NonfatalError]
    implicit val reCodec = SimpleHierarchyCodecBuilder[Response](NoTag).branch[Upsert].branch[NonfatalError].build

    def datasetEndpoint(datasetInternalName: String): Option[String] = {
      datasetInternalName.lastIndexOf('.') match {
        case -1 =>
          log.error("Could not extract data-coordinator instance name from dataset: {}", datasetInternalName)
          throw new Exception(s"Could not extract data-coordinator instance name from dataset: $datasetInternalName")
        case n =>
          hostAndPort(datasetInternalName.substring(0, n)) match {
            case Some((host, port)) => Some(s"http://$host:$port/dataset/$datasetInternalName")
            case None => None
          }
      }
    }

    // this may throw a FeedbackFailure exception
    private def postMutationScript(datasetInternalName: String, script: JArray): MutationScriptResult = {
      val endpoint = datasetEndpoint(datasetInternalName).getOrElse(return FailedToDiscoverDataCoordinator)
      val builder = RequestBuilder(new java.net.URI(endpoint))

      def body = JValueEventIterator(script)

      retrying[MutationScriptResult] {
        val start = System.nanoTime()
        httpClient.execute(builder.json(body)).run { resp =>

          def logContentTypeFailure(v: => JValue): JValue =
            try {
              v
            } catch {
              case e: ContentTypeException =>
                log.warn("The response from data-coordinator was not a valid JSON content type! The body we sent was: {}", body)
                fail("Unable to understand data-coordinator's response", e) // no retry here
            }

          resp.resultCode match {
            case 200 =>
              // success! ... well maybe...
              val end = System.nanoTime()
              log.info("Posted mutation script with {} row updates in {}ms", script.length - 2, (end - start) / 1000000)
              JsonDecode.fromJValue[JArray](logContentTypeFailure(resp.jValue())) match {
                case Right(response) =>
                  assert(response.elems.length == 1, "Response contains more than one element")
                  JsonDecode.fromJValue[JArray](response.elems.head) match {
                    case Right(results) =>
                      assert(results.elems.length == script.elems.length - 2, "Did not get one result for each upsert command that was sent")
                      val rows = script.elems.slice(2, script.elems.length)
                      results.elems.zip(rows).foreach { case (result, row) =>
                        JsonDecode.fromJValue[Response](result) match {
                          case Right(Upsert("update", _, _)) => // yay!
                          case Right(NonfatalError("error", "insert_in_update_only", Some(id))) =>
                            val JObject(fields) = JsonDecode.fromJValue[JObject](row).right.get // I just encoded this from a JObject
                            val rowId = JsonDecode.fromJValue[JString](fields(cookie.primaryKey.underlying)).right.get.string
                            if (rowId != id) return PrimaryKeyColumnHasChanged // else the row has been deleted
                          case Right(other) =>
                            fail(s"Unexpected response in array: ${other.toString}")
                          case Left(e) =>
                            fail(s"Unable to interpret result in array: ${e.english}")
                        }
                      }
                      Success
                    case Left(e) =>
                      fail(s"Row data results from data-coordinator was not an array: ${e.english}")
                  }
                case Left(e) =>
                  fail(s"Response from data-coordinator was not an array: ${e.english}")
              }
            case 400 =>
              JsonDecode.fromJValue[ErrorResponse](logContentTypeFailure(resp.jValue())) match {
                case Right(response) =>
                  response.errorCode match {
                    // { "errorCode" : "update.row.unknown-column"
                    // , "data" : { "commandIndex" : 1, "commandSubIndex" : XXX, "dataset" : "XXX.XXX", "column" : "XXXX-XXXX"
                    // }
                    case UpdateRowUnknownColumn =>
                      response.data.get("column") match {
                        case Some(JString(id)) =>
                          if (id == cookie.primaryKey.underlying)
                            PrimaryKeyColumnDoesNotExist(cookie.primaryKey)
                          else
                            TargetColumnDoesNotExist(new UserColumnId(id))
                        case Some(other) =>
                          fail(s"Unable to interpret error response from data-coordinator for status 400: $response")
                        case None =>
                          fail(s"Unable to interpret error response from data-coordinator for status 400: $response")
                      }
                    // { "errorCode" : "update.row.primary-key-nonexistent-or-null"
                    // , "data" : { "commandIndex" : 1, "dataset" : "XXX.XXX"
                    // }
                    case UpdateRowPrimaryKeyNonexistentOrNull =>
                      PrimaryKeyColumnHasChanged
                    case errorCode =>
                      fail(s"Received an unexpected error code from data-coordinator for status 400: $errorCode")
                  }
                case Left(e) =>
                  fail(s"Unable to interpret error response from data-coordinator for status 400: ${e.english}")
              }
            case 404 =>
              // { "errorCode" : "update.dataset.does-not-exist"
              // , "data" : { "dataset" : "XXX.XXX, "data" : { "commandIndex" : 0 }
              // }
              JsonDecode.fromJValue[ErrorResponse](logContentTypeFailure(resp.jValue())) match {
                case Right(response) =>
                  response.errorCode match {
                    case UpdateDatasetDoesNotExist => // awesome!
                      DatasetDoesNotExist
                    case errorCode => // awesome!
                      fail(s"Received an unexpected error code from data-coordinator for status 404: $errorCode")
                  }
                case Left(e) =>
                  fail(s"Unable to interpret error response from data-coordinator for status 404: ${e.english}")
              }
            case 409 =>
              // come back later!
              DataCoordinatorBusy // no retry
            case other =>
              if (other == 500) {
                log.warn("Scream! 500 from data-coordinator! Going to retry; my mutation script was: {}",
                  JsonUtil.renderJson(script, pretty = false)
                )
              }
              // force a retry
              throw new IOException(s"Unexpected result code $other from data-coordinator")
          }
        }
      }
    }
  }

  // to be initially thrown by postMutationScript
  case class FeedbackFailure(reason: String, cause: Throwable) extends Exception(reason, cause)

  object FeedbackFailure {

    def apply(reason: String): FeedbackFailure = FeedbackFailure(reason, null)
  }
}

/**
 * Exception type to be thrown by FeedbackSecondary.compute(.)
 */
case class ComputationFailure(reason: String, cause: Throwable) extends Exception(reason, cause)

object ComputationFailure {

  def apply(reason: String): ComputationFailure = ComputationFailure(reason, null)
}
