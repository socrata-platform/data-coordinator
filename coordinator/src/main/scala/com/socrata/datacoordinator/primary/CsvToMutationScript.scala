package com.socrata.datacoordinator.primary

import com.socrata.thirdparty.opencsv.CSVIterator
import java.io._
import com.rojoma.json.ast._
import com.rojoma.json.util.JsonUtil
import com.socrata.soql.types.{SoQLNull, SoQLType}
import com.socrata.soql.environment.TypeName
import com.socrata.datacoordinator.common.soql.SoQLRep
import java.util.zip.{GZIPOutputStream, GZIPInputStream}
import com.rojoma.json.io.CompactJsonWriter

object CsvToMutationScript extends App {
  val (inFilename, outFilename, schemaFilename, compress) = args match {
    case Array(i, o, s) => (i, o, s, false)
    case Array("-z", i, o, s) => (i, o, s, true)
  }
  val in = locally {
    val raw = new FileInputStream(inFilename)
    val cooked =
      if(compress) new GZIPInputStream(raw)
      else raw
    CSVIterator.fromInputStream(cooked)
  }

  val names = in.next()
  val schema = JsonUtil.readJsonFile[Map[String, String]](schemaFilename).getOrElse {
    sys.error("Unable to parse schema")
  }.foldLeft(Map.empty[String, SoQLType]) { (acc, fieldTyp) =>
    val (field, typ) = fieldTyp
    acc + (field -> SoQLType.typesByName(TypeName(typ)))
  }

  val out = locally {
    val filename =
      if(compress && !outFilename.endsWith(".gz")) outFilename + ".gz"
      else outFilename
    val raw = new FileOutputStream(filename)
    val cooked =
      if(compress) new GZIPOutputStream(raw)
      else raw
    new PrintWriter(new BufferedWriter(new OutputStreamWriter(cooked, "utf-8")))
  }

  def typeOf(col: String): String = schema(col).name.name

  out.println(s"""[{c:"create", user: "csv-script", locale: "en_US"}""")
  for(col <- names) {
    out.println(s""",{c:"add column", name: ${JString(col)}, type: ${JString(typeOf(col))}}""")
  }

  val csvReps = names.map { n =>
    SoQLRep.csvRep(schema(n))
  }

  val jsonRepFactory = SoQLRep.jsonRep(null, null)
  val jsonReps = names.map { n => jsonRepFactory(schema(n)) }

  val indices = names.zipWithIndex.map { nameIdx => Vector(nameIdx._2) }

  out.println(s""",{c:"row data"}""")
  while(in.hasNext) {
    val row = in.next()
    val extracted = (csvReps, indices).zipped.map { (rep, idx) =>
      rep.decode(row, idx).getOrElse(sys.error("Unable to decode " + row(idx.head) + " as " + rep.representedType))
    }
    val exported = (extracted, jsonReps).zipped.map { (v, rep) =>
      rep.toJValue(v)
    }
    val obj = (names, exported).zipped.foldLeft(Map.empty[String, JValue]) { (acc, kv) =>
      acc + (kv._1 -> kv._2)
    }
    out.print(',')
    CompactJsonWriter.toWriter(out, JObject(obj))
    out.println()
  }

  out.println("]")
  out.close()
}
