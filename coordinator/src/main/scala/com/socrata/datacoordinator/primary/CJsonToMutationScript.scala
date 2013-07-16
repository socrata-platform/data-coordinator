package com.socrata.datacoordinator.primary

import java.nio.charset.StandardCharsets.UTF_8
import com.rojoma.json.util._
import com.rojoma.json.io.{CompactJsonWriter, BlockJsonTokenIterator, JsonEventIterator}
import com.rojoma.json.codec.JsonCodec
import java.io._
import com.rojoma.json.ast.{JArray, JString, JValue}
import com.rojoma.json.ast.JString

object CJsonToMutationScript extends App {
  val in = JsonArrayIterator[JValue](new JsonEventIterator(new BlockJsonTokenIterator(new InputStreamReader(new FileInputStream(args(0)), UTF_8))))
  val out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(args(1)), UTF_8)))

  case class Field(@JsonKey("c") column: String, @JsonKey("t") typ: String)
  implicit val fieldCodec = AutomaticJsonCodecBuilder[Field]

  @JsonKeyStrategy(Strategy.Underscore)
  case class Metadata(schema: Seq[Field], rowId: Option[String], locale: String)
  implicit val metadataCodec = AutomaticJsonCodecBuilder[Metadata]

  val metadata = JsonCodec[Metadata].decode(in.next()).get

  out.println(s"""[{c:"create", user: "cjson-script", locale: ${JString(metadata.locale)}}""")
  def isSystemColumnName(s: String) = s.startsWith(":") && !s.startsWith(":@")
  for(Field(nam, typ) <- metadata.schema if !isSystemColumnName(nam)) {
    out.println(s""",{c:"add column", name: ${JString(nam)}, type: ${JString(typ)}}""")
  }
  for(rid <- metadata.rowId if !rid.startsWith(":")) {
    out.println(s""",{c:"set row id", column: ${JString(rid)}}""")
  }
  out.println(s""",{c:"row data"}""")
  for(JArray(arr) <- in) {
    out.println(metadata.schema.zip(arr).filterNot { case (c, _) => isSystemColumnName(c.column) }.map { case (k, v) =>
      JString(k.column) + ":" + CompactJsonWriter.toString(v)
    }.mkString(",{",",","}"))
  }
  out.println("]")
  out.close()
}
