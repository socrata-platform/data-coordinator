package com.socrata.datacoordinator.secondary.feedback

import java.io.IOException

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.io.{JValueEventIterator, JsonReaderException}
import com.rojoma.json.v3.util.{JsonUtil, AutomaticJsonCodecBuilder}
import com.rojoma.simplearm.Managed
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId, StrategyType}
import com.socrata.datacoordinator.secondary
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.secondary.feedback.monitor.StatusMonitor
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.http.client.{RequestBuilder, HttpClient}
import com.socrata.http.client.exceptions.{ContentTypeException, HttpClientException}

trait ComputationHandler[CT,CV, RCI <: RowComputeInfo[CV]] {

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
   * @return The RowComputeInfo's and resulting JValue's zipped with indexes of the rows
   * @note This should not throw any exception other than a [[ComputationFailure]] exception
   */
  def compute(sources: Iterator[(RCI, Int)]): Iterator[((RCI, JValue), Int)]

}

/**
 * A FeedbackSecondary is a secondary that processes updates to source columns of computed columns
 * and "feeds back" those updates to data-coordinator via posting mutation scripts.
 */
abstract class FeedbackSecondary[CT,CV, RCI <: RowComputeInfo[CV]] extends Secondary[CT,CV] {

  val log = org.slf4j.LoggerFactory.getLogger(classOf[FeedbackSecondary[CT,CV,RCI]])

  val httpClient: HttpClient
  def hostAndPort(): (String, Int)
  val baseBatchSize: Int
  val mutationScriptRetries: Int
  val computationHandler: ComputationHandler[CT, CV, RCI]
  val repFor: Array[Byte] => CT => CV => JValue
  val typeFor : CV => CT
  val statusMonitor: StatusMonitor

  /**
   * @return The batch size of mutation scripts and general processing; should be a function of `width` of dataset.
   */
  def batchSize(width: Int): Int = 40000 // TODO: figure out what this should be

  def toJValue(obfuscationKey: Array[Byte]): CV => JValue = {
    val reps = repFor(obfuscationKey);
    { value: CV =>
      reps(typeFor(value))(value)
    }
  }

  override def wantsWorkingCopies: Boolean = true

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

  /**
   * @return The `copyNumber`s of all snapshot copies in this secondary.
   */
  override def snapshots(datasetInternalName: String, cookie: Cookie): Set[Long] = Set.empty // no need for snapshots

  /**
   * In order for this secondary to drop a snapshot.  This should ignore the request
   * if the snapshot is already gone (but it should signal an error if the
   * copyNumber does not name a snapshot).
   */
  override def dropCopy(datasetInternalName: String, copyNumber: Long, cookie: Cookie): Cookie = cookie // no-op: don't have snapshots

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
      if (dataVersion != oldCookie.current.dataVersion.underlying || oldCookie.current.retriesLeft > 0) {
        val toJValueFunc = toJValue(datasetInfo.obfuscationKey)
        var newCookie = oldCookie.copy(oldCookie.current.copy(DataVersion(dataVersion)), oldCookie.previous)

        val emptyIter = Iterator[Row]()
        val toCompute = events.collect { case event =>
          event match {
            case ColumnCreated(columnInfo) =>
              val old = newCookie.current.strategyMap
              val updated = columnInfo.computationStrategyInfo match {
                case Some(strategy) =>
                  if (computationHandler.matchesStrategyType(strategy.strategyType)) old + (columnInfo.id -> strategy) else old
                case None => old
              }
              newCookie = newCookie.copy(
                current = newCookie.current.copy(
                  columnIdMap = newCookie.current.columnIdMap + (columnInfo.id -> columnInfo.systemId.underlying),
                  strategyMap = updated
                )
              )
              emptyIter
            case ColumnRemoved(columnInfo) =>
              val old = newCookie.current.strategyMap
              val updated = columnInfo.computationStrategyInfo match {
                case Some(strategy) =>
                  if (computationHandler.matchesStrategyType(strategy.strategyType)) old - columnInfo.id else old
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
                  primaryKey = new UserColumnId(":id")
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
      case error : Throwable =>
        log.warn(s"An unexpected error has occurred: ${error.getMessage}\n{}", error.getStackTrace.toString)
        Some("I'm broken, help me!")
    }
  }

  override def resync(datasetInfo: DatasetInfo, copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo[CT]], cookie: Cookie,
                      rows: Managed[Iterator[ColumnIdMap[CV]]], rollups: Seq[RollupInfo], isLatestCopy: Boolean): Cookie = {
    try {
      // decode the old cookie
      val oldCookie = FeedbackCookie.decode(cookie)

      // update cookie
      val copyNumber = CopyNumber(copyInfo.copyNumber)

      val primaryKey = schema.filter {
        case (colId, colInfo) => colInfo.isUserPrimaryKey
      }.toSeq.map({ case (_, colInfo) => colInfo.id}).head

      val columnIdMap = schema.toSeq.map({ case (colId, colInfo) => (colInfo.id, colId.underlying)}).toMap

      val strategyMap = schema.toSeq.filter {
        case (_, colInfo) => colInfo.computationStrategyInfo.exists(cs => computationHandler.matchesStrategyType(cs.strategyType))
      }.map({ case (_, colInfo) => (colInfo.id, colInfo.computationStrategyInfo.get)}).toMap

      val extra = oldCookie.map(_.current.extra).getOrElse(JNull)

      val previous = oldCookie.map(_.current)

      val cookieSchema = CookieSchema(
        dataVersion = DataVersion(copyInfo.dataVersion),
        copyNumber = copyNumber,
        primaryKey = primaryKey,
        columnIdMap = columnIdMap,
        strategyMap = strategyMap,
        retriesLeft = computationHandler.computationRetries,
        obfuscationKey = datasetInfo.obfuscationKey,
        resync = false,
        extra = extra
      )

      var newCookie = FeedbackCookie(cookieSchema, previous)

      if (isLatestCopy && newCookie.current.strategyMap.nonEmpty) {
        for {
          rws <- rows
        } {
          val toJValueFunc = toJValue(datasetInfo.obfuscationKey)
          val context = new LocalContext(newCookie, toJValueFunc)
          newCookie = context.feedback(datasetInfo.internalName, rws.map(row => Row(row, None)), resync = true)
        }
      }

      FeedbackCookie.encode(newCookie)
    } catch {
      case replayLater:ReplayLaterSecondaryException =>
        throw replayLater
      case error : Throwable =>
        // note: shouldn't ever throw a ResyncSecondaryException from inside resync
        log.warn(s"An unexpected error has occurred: ${error.getMessage}\n{}", error.getStackTrace.toString)
        Some("I'm broken, help me!")
    }
  }

  case class Row(data: secondary.Row[CV], oldData: Option[secondary.Row[CV]])

  private class LocalContext(feedbackCookie: FeedbackCookie, toJValueFunc: CV => JValue) {

    val cookie = feedbackCookie.current

    // this may throw a ResyncSecondaryException or a ReplayLaterSecondaryException
    def feedback(datasetInternalName: String, rows: Iterator[Row], resync: Boolean = false): FeedbackCookie = {
      val width = cookie.columnIdMap.size
      val size = batchSize(width)

      log.info("Processing update of dataset {} to version {} with batch_size = {}",
        datasetInternalName, cookie.dataVersion.underlying.toString, size.toString)

      var count = 0
      try {
        rows.grouped(size).foreach { batch =>
          count += 1
          val updates = computeUpdates(batch.toIterator)
          val script = writeMutationScript(updates)
          val result = postMutationScript(datasetInternalName, script)
          result match {
            case Success =>
              log.info("Finished batch {} of approx. {} rows", count, size)
              statusMonitor.update(datasetInternalName, cookie.dataVersion, size, count)
            case ddne@DatasetDoesNotExist =>
              log.info("Completed updating dataset {} to version {} early: {}", datasetInternalName, cookie.dataVersion.underlying.toString, ddne.english)
              statusMonitor.remove(datasetInternalName, cookie.dataVersion, count)

              return feedbackCookie.copy(current = cookie.copy(retriesLeft = 0, resync = resync)) // nothing more to do and no need to try
            case schemaChange =>
              log.info("A schema change occurred breaking my upsert: {}; going to start a resync...", schemaChange.english)
              throw ResyncSecondaryException(s"A schema change occurred breaking my upsert: ${schemaChange.english}")
          }
        }
      } catch {
        case ComputationFailure(reason, cause) =>
          // Some failure has occurred in computation; we will only retry these exceptions so many times
          val newCookie = feedbackCookie.copy(
            current = cookie.copy(retriesLeft = cookie.retriesLeft - 1) // decrement retries
          )
          throw ReplayLaterSecondaryException(reason, FeedbackCookie.encode(newCookie), cause)
        case FeedbackFailure(reason, cause) =>
          // We failed to post our mutation script to data-coordinator for some reason; we will retry this indefinitely
          throw ReplayLaterSecondaryException(reason, FeedbackCookie.encode(feedbackCookie), cause) // do not decrement retries left
      }
      log.info("Completed updating dataset {} to version {} after batch: {}",
        datasetInternalName, cookie.dataVersion.underlying.toString, count.toString)
      statusMonitor.remove(datasetInternalName, cookie.dataVersion, count)

      feedbackCookie.copy(current = cookie.copy(retriesLeft = 0, resync = resync)) // do not need to try
    }

    // this may throw a ComputationFailure exception
    private def computeUpdates(rows: Iterator[Row]): Iterator[JObject] = {
      val indexedRows = rows.zipWithIndex
      val strategies = cookie.strategyMap.toSeq
      val toCompute = indexedRows.flatMap { case (row, index) =>
        strategies.map { case (targetColId: UserColumnId, strategy) =>
          // don't compute if there has been to change to the source columns
          if (noChange(row, strategy.sourceColumnIds))
            None
          else
            Some((computationHandler.transform(row.data, targetColId, strategy, cookie), index))
        }.filter(x => x.isDefined).map(x => x.get)
      }

      val results = unflatten(computationHandler.compute(toCompute))

      results.map { case (row, updates) =>
        val filtered = updates.filter { case (colId: UserColumnId, value) =>
          extractJValue(row, colId) != value // don't update to current value
        }
        val rowId = extractJValue(row, cookie.primaryKey)
        if (filtered.nonEmpty) {
          Some(JObject(Map(
            cookie.primaryKey.underlying -> rowId
          ) ++ filtered.map { case (id, value) => (id.underlying, value)}.toMap
          ))
        } else {
          None
        }
      }.filter(x => x.isDefined).map(x => x.get)
    }

    private def noChange(row: Row, columns: Seq[UserColumnId]): Boolean = row.oldData match {
      case Some(old) =>
        columns.forall { id =>
          val internal = new ColumnId(cookie.columnIdMap(id))
          row.data(internal) == old(internal)
        }
      case None => false
    }

    private def unflatten(values: Iterator[((RCI, JValue), Int)]): Iterator[(secondary.Row[CV], Seq[(UserColumnId, JValue)])] = {
      val results = scala.collection.mutable.ListBuffer[(secondary.Row[CV], Seq[(UserColumnId, JValue)])]()
      val buffered = values.buffered
      var seq = Seq[(UserColumnId, JValue)]()
      for (((rci, v), i) <- buffered) {
        seq = seq :+ ((rci.targetColId, v))
        if (!buffered.hasNext || buffered.head._2 > i) {
          results += ((rci.data, seq))
          seq = Seq[(UserColumnId, JValue)]()
        }
      }

      results.toIterator
    }

    private def extractJValue(row: secondary.Row[CV], userId: UserColumnId) =
      toJValueFunc(row(new ColumnId(cookie.columnIdMap(userId))))

    // commands for mutation scripts
    val commands = Seq(
      JObject(Map(
        "c" -> JString("normal"), "user" -> JString(computationHandler.user)
      )),
      JObject(Map(
        "c" -> JString("row data"), "update_only" -> JBoolean.canonicalTrue, "nonfatal_row_errors" -> JArray(Array(JString("insert_in_update_only")))
      ))
    )

    private def writeMutationScript(rowUpdates: Iterator[JObject]): JArray = JArray(commands ++ rowUpdates)

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

    private def retrying[T](actions: => T, remainingAttempts: Int = mutationScriptRetries): T = {
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
    implicit val reCodec = AutomaticJsonCodecBuilder[Response]

    def datasetEndpoint: String = {
      val (host, port) = hostAndPort
      s"http://$host:$port/dataset/"
    }

    // this may throw a FeedbackFailure exception
    private def postMutationScript(datasetInternalName: String, script: JArray): MutationScriptResult = {
      val builder = RequestBuilder(new java.net.URI(datasetEndpoint + datasetInternalName))

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
              log.info("Posted mutation script with {} row updates in {}ms", script.length, (end - start) / 1000000)
              JsonDecode.fromJValue[JArray](logContentTypeFailure(resp.jValue())) match {
                case Right(response) =>
                  assert(response.elems.length == 1)
                  JsonDecode.fromJValue[JArray](response) match {
                    case Right(results) =>
                      assert(results.elems.length == script.elems.length - 2)
                      val rows = script.elems.slice(2, script.elems.length)
                      results.elems.zip(rows).foreach { case (result, row) =>
                        JsonDecode.fromJValue[Response](result) match {
                          case Right(Upsert("update", _, _)) => // yay!
                          case Right(NonfatalError("error", "insert_in_update_only", Some(id))) =>
                            val JObject(fields) = JsonDecode.fromJValue[JObject](row).right.get // I just encoded this from a JObject
                          val rowId = JsonDecode.fromJValue[JString](fields(cookie.primaryKey.underlying)).right.get.string
                            if (rowId != id) return PrimaryKeyColumnHasChanged // else the row has been deleted
                          case Right(other) =>
                            fail(s"Unexpected response in array: $other")
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
              throw fail("Received 409 from data-coordinator.") // no retry
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
