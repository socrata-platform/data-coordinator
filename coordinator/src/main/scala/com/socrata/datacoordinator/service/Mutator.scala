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

  // User-causable failure cases:
  //  * command stream is empty
  //  * command stream's first element is not a context establishment
  //    - or it is a "create" of a dataset that already exists
  //    - or it is any other operation on a dataset that DOESN'T exist
  //    - or it is copy/publish/drop on a dataset whose latest copy is the wrong lifecycle stage
  //  * something else is holding dataset's write lock long enough to time out
  //  * subsequent element is not a valid operation
  //  * operation fails:
  //    - column add
  //       + column with name already exists
  //       + illegal name (e.g., system column)
  //       + unknown type
  //    - column rename
  //       + column with source name doesn't exist
  //       + column with target name already exists
  //       + source is system column
  //       + illegal target name (e.g., system column)
  //    - column drop
  //       + column doesn't exist
  //       + system column
  //    - make primary key
  //       + unknown column
  //       + column doesn't have a PKable type
  //       + system column
  //    - remove primary key
  //       + Some other column is user PK
  //       + NO column is user PK
  //    - row data, if "abort on data error" is true
  //       + no such id to delete
  //       + no such id to upsert (and no user PK is defined)
  //       + no id provided (and user PK is defined)
  //       + unable to convert data to column type
  //    - row data, unconditional errors
  //       + (can't think of any?)
  //
  // Explicitly NOT failure cases:
  //  * operations:
  //    - row data
  //       + system columns provided (we sanitize them)
  //       + unknown column in object
  //
  // Uncertainty:
  //  * Should DDL-on-working-copies-only be enforced at this level?
  //     - I think yes
  //  * Should we enforce "you must perform SOME kind of DDL in order to publish a working copy?"
  //     - I think.. maybe?  I don't want people to unnecessarily create working copies!
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
