package com.socrata.datacoordinator.loader
package loaderperf

import scala.{collection => sc}

import java.sql.{SQLException, Connection, DriverManager}

import com.rojoma.simplearm.util._
import java.io.{FileInputStream, InputStreamReader, BufferedReader}
import com.rojoma.json.io.{BlockJsonTokenIterator, JsonEventIterator, JsonReader}
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JValue, JNull, JNumber, JString}
import java.util.zip.GZIPInputStream
import com.socrata.id.numeric.{InMemoryBlockIdProvider, FixedSizeIdProvider, PushbackIdProvider}

class ExecutePlan

object ExecutePlan {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[ExecutePlan])

  val trialsPerDataPoint = 10

  val EndOfSection = JString("------")

  def time[T](label: String)(f: => T): T = {
    log.info("Starting " + label)
    val start = System.nanoTime()
    val result = f
    val end = System.nanoTime()
    log.info("Finished " + label + " (" + ((end - start) / 1000000) + "ms)")
    result
  }

  def main(args: Array[String]) {
    try {
      val datapoints = for(trial <- 1 to trialsPerDataPoint) yield {
        val idProvider = new PushbackIdProvider(new FixedSizeIdProvider(new InMemoryBlockIdProvider(releasable = false), 1000))
        for {
          planReader <- managed(new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(args(0))), "UTF-8")))
          conn <- managed(DriverManager.getConnection("jdbc:postgresql:robertm", "robertm", "lof9afw3"))
        } yield {
          val plan = new JsonReader(new JsonEventIterator(new BlockJsonTokenIterator(planReader, blockSize = 10240)))
          implicit def connImplict = conn

          conn.setAutoCommit(false)
          executeDDL("DROP TABLE IF EXISTS perf_data")
          executeDDL("DROP TABLE IF EXISTS perf_log")
          val schema = JsonCodec.fromJValue[Map[String, String]](plan.read()).getOrElse(sys.error("Cannot read schema"))
          val createSql = schema.toSeq.map { case (col, typ) => "u_" + col + " " + typ }.mkString("CREATE TABLE perf_data (id bigint not null primary key,",",",")")
          log.info(createSql)
          executeDDL(createSql)
          executeDDL("ALTER TABLE perf_data ALTER COLUMN u_uid SET NOT NULL")
          executeDDL("CREATE UNIQUE INDEX perf_data_uid ON perf_data(u_uid)")
          executeDDL("ALTER TABLE perf_data ADD UNIQUE USING INDEX perf_data_uid")
          executeDDL("CREATE TABLE perf_log (id bigint not null primary key, rows text not null, who varchar(14) not null)")

          val userSchema = schema.mapValues {
            case "TEXT" => PTText
            case "NUMERIC" => PTNumber
          }.toMap
          val datasetContext = new PerfDatasetContext("perf", userSchema, Some("uid"))
          val sqlizer = new PerfDataSqlizer("me", datasetContext)

          time("Prepopulating") {
            using(conn.prepareStatement("INSERT INTO perf_data (id," + schema.keys.toSeq.map("u_"+).mkString(",") + ") SELECT ?," + schema.values.map(_ => "?").mkString(","))) { stmt =>
              def loop(i: Int = 0) {
                val line = plan.read()
                if(line != EndOfSection) {
                  val ins = JsonCodec.fromJValue[Insert](line).getOrElse(sys.error("Cannot read insert"))
                  // stmt.addBatch(insert(ins.id, ins.fields))
                  stmt.setLong(1, idProvider.allocate())
                  var j = 2
                  for((k,t) <- schema) {
                    ins.fields(k) match {
                      case JString(s) => stmt.setString(j, s)
                      case JNumber(n) => stmt.setBigDecimal(j, n.underlying)
                      case JNull => if(t == "TEXT") stmt.setNull(j, java.sql.Types.VARCHAR) else stmt.setNull(j, java.sql.Types.NUMERIC)
                      case other => sys.error("Unexpected JSON datum " + other)
                    }
                    j += 1
                  }

                  stmt.addBatch()
                  if(i == 10000) {
                    val results = time("Sending prepopulation batch") {
                      stmt.executeBatch()
                    }
                    assert(results.forall(_ == 1))
                    loop(0)
                  } else {
                    loop(i + 1)
                  }
                } else if(i != 0) {
                  val results = time("Sending final prepopulation batch") {
                    stmt.executeBatch()
                  }
                  assert(results.forall(_ == 1))
                }
              }
              loop()
            }
            time("Committing prepopulation") {
              conn.commit()
            }
          }
          conn.setAutoCommit(true)
          time("Analyzing") {
            executeDDL("VACUUM ANALYZE perf_data")
          }
          conn.setAutoCommit(false)

          log.info("Executing...")

          def convertValue(field: String, in: JValue): PerfValue = {
            if(in == JNull) PVNull
            else {
              userSchema(field) match {
                case PTText => PVText(in.asInstanceOf[JString].string)
                case PTNumber => PVNumber(in.asInstanceOf[JNumber].number)
              }
            }
          }

          def convert(row: sc.Map[String, JValue]): Map[String, PerfValue] = {
            row.foldLeft(Map.empty[String, PerfValue]) { (result, kv) =>
              val (k,v) = kv
              result + (k -> convertValue(k, v))
            }
          }

          val start = System.nanoTime()
          val report = using(PostgresTransaction(conn, PerfTypeContext, sqlizer, idProvider)) { txn =>
            def loop() {
              val line = plan.read()
              if(line != EndOfSection) {
                val op = JsonCodec.fromJValue[Operation](line).getOrElse(sys.error("Can't decode op"))
                op match {
                  case Insert(_, row) => txn.upsert(convert(row.fields))
                  case Update(_, row) => txn.upsert(convert(row.fields))
                  case Delete(_, id) => txn.delete(convertValue("uid", id))
                }
                loop()
              }
            }
            loop()
            val r = txn.report
            txn.commit()
            r
          }
          val end = System.nanoTime()

          log.info("inserted: " + report.inserted.size + "; updated: " + report.updated.size + "; deleted: " + report.deleted.size + "; elided: " + report.elided.size + "; errors: " + report.errors.size)
          if(report.errors != 0) {
            report.errors.toSeq.sortBy(_._1).take(20).foreach { x => log.info(x.toString) }
          }

          val delta = end - start
          delta
        }
      }
      val avg = datapoints.sum / trialsPerDataPoint
      log.info("Average runtime (upsert process only): " + (avg / 1000000))
    } catch {
      case e: SQLException =>
        if(e.getNextException != null) {
          e.getNextException.printStackTrace()
        } else {
          e.printStackTrace()
        }
    }
  }

  def executeDDL(sql: String)(implicit conn: Connection) {
    using(conn.createStatement()) { stmt =>
      stmt.execute(sql)
    }
  }

  def executeUpdate(sql: String)(implicit conn: Connection): Int = {
    using(conn.createStatement()) { stmt =>
      stmt.execute(sql)
      stmt.getUpdateCount
    }
  }
}
