package com.socrata.querycoordinator

import com.rojoma.json.v3.ast.{JObject, JString}
import com.socrata.soql.types.{SoQLNumber, SoQLText}
import org.scalatest._

class QueryExecutorTest extends FunSuite with ShouldMatchers {
  // TODO: test apply(...)

  private def schemaEquivalent(a: Schema, b: Schema): Boolean =
    a.hash == b.hash && a.pk == b.pk && a.schema == b.schema

  test("check schema hash mismatch") {
    val schema = Schema("dead-beef", Map("name" -> SoQLText, "age" -> SoQLNumber), "pri-key")
    val obj = JObject(Map(
      "errorCode" -> JString("internal.schema-mismatch"),
      "data" -> Schema.SchemaCodec.encode(schema)
    ))

    schemaEquivalent(QueryExecutor.checkSchemaHashMismatch(obj).get, schema) should be(true)
  }

  test("check schema hash mismatch - other errors yield none") {
    val schema = Schema("dead-beef", Map("name" -> SoQLText, "age" -> SoQLNumber), "pri-key")
    val obj = JObject(Map(
      "errorCode" -> JString("out-of-memory"),
      "data" -> Schema.SchemaCodec.encode(schema)
    ))

    QueryExecutor.checkSchemaHashMismatch(obj) should be(None)
  }

  test("check schema hash mismatch - schema decode error yield none") {
    val obj = JObject(Map(
      "errorCode" -> JString("internal.schema-mismatch"),
      "data" -> JString("this isn't a Schema JObject")
    ))

    QueryExecutor.checkSchemaHashMismatch(obj) should be(None)
  }
}
