package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.util.JsonUtil

import org.scalatest.FunSuite
import org.scalatest.MustMatchers
import com.socrata.soql.types.SoQLJson
import com.socrata.soql.types.SoQLNull

class SoQJsonRepTest extends FunSuite with MustMatchers {
  test("parse json") {
    JsonRep.fromJValue(JsonUtil.parseJson[JValue]("{}").right.get) must equal(None)
    JsonRep.fromJValue(JsonUtil.parseJson[JValue]("{\"json\": 7}").right.get).get must equal(SoQLJson(JNumber(7)))
    JsonRep.fromJValue(JsonUtil.parseJson[JValue]("{\"json\": null}").right.get).get must equal(SoQLJson(JNull))
    JsonRep.fromJValue(JNull).get must equal(SoQLNull)
  }

  test("write json") {
    JsonUtil.renderJson(JsonRep.toJValue(SoQLJson(JNull))) must equal("{\"json\":null}")
    JsonUtil.renderJson(JsonRep.toJValue(SoQLJson(JNumber(7)))) must equal("{\"json\":7}")
    JsonUtil.renderJson(JsonRep.toJValue(SoQLNull)) must equal("null")
  }
}
