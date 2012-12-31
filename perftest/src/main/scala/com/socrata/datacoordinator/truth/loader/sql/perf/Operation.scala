package com.socrata.datacoordinator
package truth.loader
package sql
package perf

import com.rojoma.json.util.{InternalTag, SimpleHierarchyCodecBuilder, AutomaticJsonCodecBuilder}
import com.rojoma.json.ast.{JValue, JObject}

sealed abstract class Operation {
  def id: Long
  def isUpsert: Boolean
}

case class Insert(id: Long, fields: JObject) extends Operation {
  def isUpsert = true
}
object Insert {
  implicit val jCodec = AutomaticJsonCodecBuilder[Insert]
}

case class Update(id: Long, fields: JObject) extends Operation {
  var runOnce: Boolean = false
  def isUpsert = true
}
object Update {
  implicit val jCodec = AutomaticJsonCodecBuilder[Update]
}

case class Delete(id: Long, uid: JValue) extends Operation {
  def isUpsert = false
}
object Delete {
  implicit val jCodec = AutomaticJsonCodecBuilder[Delete]
}

object Operation {
  implicit val jCodec = SimpleHierarchyCodecBuilder[Operation](InternalTag("t")).
    branch[Insert]("i").
    branch[Update]("u").
    branch[Delete]("d").
    build
}
