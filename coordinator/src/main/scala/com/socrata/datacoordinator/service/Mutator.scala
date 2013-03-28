package com.socrata.datacoordinator.service

import com.rojoma.json.ast.JValue
import com.socrata.datacoordinator.truth.universe.{DatasetMutatorProvider, Universe}
import com.socrata.datacoordinator.truth.DatasetMutator
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.json.JsonColumnReadRep

class Mutator[CT, CV](repFor: ColumnInfo => JsonColumnReadRep[CT, CV]) {
  sealed abstract class StreamType
  case object NormalMutation extends StreamType
  case object CreateDatasetMutation extends StreamType
  case class CreateWorkingCopyMutation(copyData: Boolean) extends StreamType
  case class PublishWorkingCopyMutation(keepingSnapshotCount: Int) extends StreamType
  case object DropWorkingCopyMutation extends StreamType

  sealed abstract class Command {
    def operate(mutator: DatasetMutator[CV]#MutationContext)
  }

  class CommandStream(commandStream: Iterator[JValue]) {
    def streamType: StreamType = ???
    def datasetName: String = ???

    def decodeCommand(json: JValue): Command = ???

    def foreach[U](f: Command => U) =
      commandStream.map(decodeCommand).foreach(f)
  }

  // Failure cases:
  //  * iterator is empty
  //  * iterator's first element is not a context establishment
  //
  def apply(u: Universe[CT, CV] with DatasetMutatorProvider, commandStream: Iterator[JValue], user: String) {
    val commands = new CommandStream(commandStream)
    commands.streamType match {
      case NormalMutation =>
        for(ctx <- u.datasetMutator.openDataset(user)(commands.datasetName)) {
          val mutator = ctx.getOrElse { ??? /* TODO: Not found */ }
          carryOutCommands(mutator, commands)
        }
      case CreateDatasetMutation =>
        for(mutator <- u.datasetMutator.createDataset(user)(commands.datasetName, "t")) { // TODO: Already exists
          carryOutCommands(mutator, commands)
        }
      case CreateWorkingCopyMutation(copyData) =>
        for(ctx <- u.datasetMutator.createCopy(user)(commands.datasetName, copyData = copyData)) {
          val mutator = ctx.getOrElse { ??? /* TODO: Not found */ }
          carryOutCommands(mutator, commands)
        }
      case PublishWorkingCopyMutation(keepingSnapshotCount) =>
        for(ctx <- u.datasetMutator.publishCopy(user)(commands.datasetName)) {
          val mutator = ctx.getOrElse { ??? /* TODO: Not found */ }
          carryOutCommands(mutator, commands)
        }
      case DropWorkingCopyMutation =>
        for(ctx <- u.datasetMutator.dropCopy(user)(commands.datasetName)) {
          val mutator = ctx.getOrElse { ??? /* TODO: Not found */ }
          carryOutCommands(mutator, commands)
        }
    }
  }

  def carryOutCommands(mutator: DatasetMutator[CV]#MutationContext, commands: CommandStream) {
    commands.foreach(_.operate(mutator))
  }
}
