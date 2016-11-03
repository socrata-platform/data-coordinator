package com.socrata.datacoordinator.secondary.messaging

import com.rojoma.json.v3.ast.JString
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.util._

case class ViewUid(uid: String)

object ViewUid {
  implicit object encode extends JsonEncode[ViewUid] {
    def encode(s: ViewUid) = JString(s.uid)
  }
}

object ToViewUid {
  // drop the leading '_' on resource_name to convert to view uid
  def apply(resourceName: String): ViewUid = ViewUid(resourceName.drop(1))
}

sealed abstract class Message

@JsonKeyStrategy(Strategy.Underscore)
case class StoreVersion(viewUid: ViewUid,
                        groupName: Option[String],
                        storeId: String,
                        newDataVersion: Long,
                        startingAtMs: Long,
                        endingAtMs: Long) extends Message

object StoreVersion {
  implicit val encode = AutomaticJsonEncodeBuilder[StoreVersion]
}

@JsonKeyStrategy(Strategy.Underscore)
case class GroupVersion(viewUid: ViewUid,
                        groupName: String,
                        storeIds: Set[String],
                        newDataVersion: Long,
                        endingAtMs: Long) extends Message

object GroupVersion {
  implicit val encode = AutomaticJsonEncodeBuilder[GroupVersion]
}

object Message {
  implicit val encode = SimpleHierarchyEncodeBuilder[Message](NoTag)
    .branch[StoreVersion]
    .branch[GroupVersion]
    .build
}
