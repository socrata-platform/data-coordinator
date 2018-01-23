package com.socrata.datacoordinator.service.collocation

import java.util.UUID

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode}
import com.rojoma.json.v3.util.AutomaticJsonDecodeBuilder
import com.socrata.datacoordinator.external.CollocationError
import com.socrata.datacoordinator.id.{DatasetId, DatasetInternalName}
import com.socrata.datacoordinator.resources.collocation._
import com.socrata.datacoordinator.resources.SecondariesOfDatasetResult
import com.socrata.datacoordinator.secondary.config.SecondaryGroupConfig
import com.socrata.http.client._

case class CoordinatorError(resultCode: String, data: JObject)

object CoordinatorError {
  implicit val decode = AutomaticJsonDecodeBuilder[CoordinatorError]
}

sealed abstract class ErrorResult(message: String) extends Exception(message)

sealed abstract class RequestError(message: String) extends ErrorResult(message)

case class ResponseError(error: DecodeError) extends RequestError(error.english)
case class UnexpectedError(message: String) extends RequestError(message)
case class UnexpectedCoordinatorError(errorCode: String) extends RequestError(s"Unexpected result code $errorCode from other coordinator")
case class InstanceNotFound(name: String) extends RequestError(s"Could not find host and port for data-coordinator.$name")

sealed abstract class ResourceNotFound(name: String, resource: String) extends ErrorResult(s"No such $resource $name")

case class StoreGroupNotFound(name: String) extends ResourceNotFound(name, "secondary store group")
case class StoreNotFound(name: String) extends ResourceNotFound(name, "secondary store")
case class DatasetNotFound(internalName: DatasetInternalName) extends ResourceNotFound(internalName.underlying, "dataset")

trait Coordinator {
  def secondaryGroups: Map[String, SecondaryGroupConfig]
  def collocatedDatasetsOnInstance(instance: String, datasets: Set[DatasetInternalName]): Either[RequestError, CollocatedDatasetsResult]
  def secondariesOfDataset(internalName: DatasetInternalName): Either[RequestError, Option[SecondariesOfDatasetResult]]
  def secondaryMoveJobs(storeGroup: String, internalName: DatasetInternalName): Either[ErrorResult, SecondaryMoveJobsResult]
  def ensureSecondaryMoveJob(storeGroup: String, internalName: DatasetInternalName, request: SecondaryMoveJobRequest): Either[ErrorResult, Either[InvalidMoveJob, Boolean]]
  def rollbackSecondaryMoveJob(instance: String, jobId: UUID, move: Move, dropFromStore: Boolean): Option[ErrorResult]
}

class HttpCoordinator(isThisInstance: String => Boolean,
                      val secondaryGroups: Map[String, SecondaryGroupConfig],
                      hostAndPort: String => Option[(String, Int)],
                      httpClient: HttpClient,
                      collocatedDatasets: Set[DatasetInternalName] => CollocatedDatasetsResult,
                      secondariesOfDataset: DatasetId => Option[SecondariesOfDatasetResult],
                      secondaryMoveJobs: (String, DatasetId) => Either[ResourceNotFound, SecondaryMoveJobsResult],
                      ensureSecondaryMoveJob: (String, DatasetId, SecondaryMoveJobRequest) => Either[ResourceNotFound, Either[InvalidMoveJob, Boolean]],
                      rollbackSecondaryMoveJob: (DatasetId, SecondaryMoveJobRequest, Boolean) => Option[DatasetNotFound]) extends Coordinator {

  private val log = org.slf4j.LoggerFactory.getLogger(classOf[HttpCoordinator])

  private def doRequest[T](instance: String, route: String, request: RequestBuilder => SimpleHttpRequest)
                          (handleResponse: Response => Either[RequestError, T]): Either[RequestError, T] = {
    hostAndPort(instance) match {
      case Some((host, port)) =>
        val builder = RequestBuilder(new java.net.URI(s"http://$host:$port$route"))

        httpClient.execute(request(builder)).run(handleResponse)
      case None =>
        log.warn("Unable to discover other instance {}", instance)
        Left(InstanceNotFound(instance))
    }
  }

  private def request[T : JsonDecode](instance: String, route: String)
                                     (request: RequestBuilder => SimpleHttpRequest): Either[RequestError, Option[T]] = {
    doRequest[Option[T]](instance, route, request) { response: Response =>
      response.resultCode match {
        case 200 | 201 =>
          response.value[T]() match {
            case Right(result) => Right(Some(result))
            case Left(error) =>
              log.error("Unable to parse response from other instance {} for status {}: {}",
                instance, response.resultCode.toString, error.english)
              Left(ResponseError(error))
          }
        case 404 => Right(None)
        case resultCode =>
          val message = s"Received unexpected result code $resultCode from other instance $instance!"
          log.error(message)
          Left(UnexpectedError(message))
      }
    }
  }

  override def collocatedDatasetsOnInstance(instance: String, datasets: Set[DatasetInternalName]): Either[RequestError, CollocatedDatasetsResult] = {
    if (isThisInstance(instance)) {
      Right(collocatedDatasets(datasets))
    } else {
      val route = s"/collocation-manifest/$instance"
      request[CollocatedDatasetsResult](instance, route)(_.jsonBody(datasets)) match {
        case Right(Some(result)) => Right(result)
        case Right(None) => Left(InstanceNotFound(instance))
        case Left(error) => Left(error)
      }
    }
  }

  override def secondariesOfDataset(internalName: DatasetInternalName): Either[RequestError, Option[SecondariesOfDatasetResult]] = {
    if (isThisInstance(internalName.instance)) {
      Right(secondariesOfDataset(internalName.datasetId))
    } else {
      val route = s"/secondaries-of-dataset/${internalName.underlying}"
      request[SecondariesOfDatasetResult](internalName.instance, route)(_.get)
    }
  }

  private def secondaryMoveRoute(storeGroup: Option[String], internalName: DatasetInternalName) =
  s"/secondary-manifest/move/${storeGroup.map(_ + "/").getOrElse("")}${internalName.underlying}"

  override def secondaryMoveJobs(storeGroup: String, internalName: DatasetInternalName): Either[ErrorResult, SecondaryMoveJobsResult] = {
    if (isThisInstance(internalName.instance)) {
      secondaryMoveJobs(storeGroup, internalName.datasetId) match {
        case Right(result) => Right(result)
        case Left(resourceNotFound) => Left(resourceNotFound)
      }
    } else {
      request[SecondaryMoveJobsResult](internalName.instance, secondaryMoveRoute(Some(storeGroup), internalName))(_.get) match {
        case Right(Some(result)) => Right(result)
        case Right(None) => Left(DatasetNotFound(internalName))
        case Left(requestError) => Left(requestError)
      }
    }
  }

  override def ensureSecondaryMoveJob(storeGroup: String,
                                      internalName: DatasetInternalName,
                                      request: SecondaryMoveJobRequest): Either[ErrorResult, Either[InvalidMoveJob, Boolean]] = {
    if (secondaryGroups.contains(storeGroup)) {
      if (isThisInstance(internalName.instance)) {
        ensureSecondaryMoveJob(storeGroup, internalName.datasetId, request)
      } else {
        val instance = internalName.instance
        doRequest[Either[ResourceNotFound, Either[InvalidMoveJob, Boolean]]](instance, secondaryMoveRoute(Some(storeGroup), internalName), _.jsonBody(request)) { response =>
          response.resultCode match {
            case 200 => response.value[Boolean]() match {
              case Right(result) => Right(Right(Right(result)))
              case Left(error) => Left(ResponseError(error))
            }
            case 400 => response.value[CoordinatorError]() match {
              case Right(CoordinatorError(CollocationError.STORE_NOT_ACCEPTING_NEW_DATASETS, _)) =>
                Right(Right(Left(StoreNotAcceptingDatasets)))
              case Right(CoordinatorError(CollocationError.DATASET_NOT_FOUND_IN_STORE, _)) =>
                Right(Right(Left(DatasetNotInStore)))
              case Right(error) => Left(UnexpectedError(s"Unexpected error of type ${error.getClass.getName}"))
              case Left(error) => Left(ResponseError(error))
            }
            case 404 => response.value[CoordinatorError]() match {
              case Right(CoordinatorError(CollocationError.STORE_GROUP_DOES_NOT_EXIST, _)) =>
                Right(Left(StoreGroupNotFound(storeGroup)))
              case Right(CoordinatorError(CollocationError.DATASET_DOES_NOT_EXIST, _)) =>
                Right(Left(DatasetNotFound(internalName)))
              case Right(error) => Left(UnexpectedError(s"Unexpected error of type ${error.getClass.getName}"))
              case Left(error) => Left(ResponseError(error))
            }
            case resultCode =>
              val message = s"Received unexpected result code $resultCode from other instance $instance!"
              log.error(message)
              Left(UnexpectedError(message))
          }
        } match {
          case Right(Right(result)) => Right(result)
          case Right(Left(resourceNotFound)) => Left(resourceNotFound)
          case Left(requestError) => Left(requestError)
        }
      }
    } else {
      Left(StoreGroupNotFound(storeGroup))
    }
  }

  override def rollbackSecondaryMoveJob(instance: String, jobId: UUID, move: Move, dropFromStore: Boolean): Option[ErrorResult] = {
    val request = SecondaryMoveJobRequest(jobId, move.storeIdFrom, move.storeIdTo)
    if (isThisInstance(instance)) {
      rollbackSecondaryMoveJob(move.datasetInternalName.datasetId, request, dropFromStore)
    } else {
      val parameters = Seq(
        ("rollback", "true"),
        ("dropFromStore", dropFromStore.toString)
      )
      doRequest[Option[ErrorResult]](instance, secondaryMoveRoute(None, move.datasetInternalName),
        _.addParameters(parameters).jsonBody(request)) { response =>
        response.resultCode match {
          case 200 => Right(None) // success
          case 404 => Right(Some(DatasetNotFound(move.datasetInternalName)))
          case resultCode =>
            val message = s"Received unexpected result code $resultCode from other instance $instance!"
            log.error(message)
            Right(Some(UnexpectedError(message)))
        }
      }
    } match {
      case Right(None) => None
      case Right(Some(error)) => Some(error)
      case Left(error) => Some(error)
    }
  }
}
