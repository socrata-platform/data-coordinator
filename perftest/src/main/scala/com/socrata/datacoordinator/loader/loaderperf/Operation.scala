package com.socrata.datacoordinator.loader.loaderperf

import com.rojoma.json.util.{InternalTag, SimpleHierarchyCodecBuilder, SimpleJsonCodecBuilder}
import com.rojoma.json.ast.{JValue, JObject}

sealed abstract class Operation {
  def id: Long
  def isUpsert: Boolean
}

case class Insert(id: Long, fields: JObject) extends Operation {
  def isUpsert = true
}
object Insert {
  implicit val jCodec = SimpleJsonCodecBuilder[Insert].build("id", _.id, "fields", _.fields)
}

case class Update(id: Long, fields: JObject) extends Operation {
  var runOnce: Boolean = false
  def isUpsert = true
}
object Update {
  implicit val jCodec = SimpleJsonCodecBuilder[Update].build("id", _.id, "fields", _.fields)
}

case class Delete(id: Long, uid: JValue) extends Operation {
  def isUpsert = false
}
object Delete {
  implicit val jCodec = SimpleJsonCodecBuilder[Delete].build("id", _.id, "uid", _.uid)
}

object Operation {
  implicit val jCodec = SimpleHierarchyCodecBuilder[Operation](InternalTag("t")).
    branch[Insert]("i").
    branch[Update]("u").
    branch[Delete]("d").
    build
}
