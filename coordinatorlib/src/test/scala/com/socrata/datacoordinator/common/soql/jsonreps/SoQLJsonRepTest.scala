package com.socrata.datacoordinator.common.soql.jsonreps

import com.rojoma.json.v3.codec.{JsonDecode, DecodeError}
import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.interpolation._

import org.scalatest.FunSuite
import org.scalatest.MustMatchers
import com.socrata.soql.types.SoQLJson
import com.socrata.soql.types.SoQLNull

class SoQLJsonRepTest extends FunSuite with MustMatchers {
  test("parse json") {
    SoQLJson.cjsonRep.fromJValue(j"{}") must equal(Left(DecodeError.MissingField("json")))
    SoQLJson.cjsonRep.fromJValue(j"""{"json": 7}""") must equal(Right(Some(SoQLJson(JNumber(7)))))
    SoQLJson.cjsonRep.fromJValue(j"""{"json": null}""") must equal(Right(Some(SoQLJson(JNull))))
    SoQLJson.cjsonRep.fromJValue(JNull) must equal(Right(None))
  }

  test("write json") {
    SoQLJson.cjsonRep.toJValue(SoQLJson(JNull)) must equal(j"""{"json":null}""")
    SoQLJson.cjsonRep.toJValue(SoQLJson(JNumber(7))) must equal(j"""{"json":7}""")
  }
}
