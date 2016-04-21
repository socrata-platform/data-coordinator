package com.socrata.datacoordinator.resources

import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.ast.JArray
import com.rojoma.json.v3.util.ArrayIteratorEncode
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.loader.{MissingVersion, Delogger}
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._

case class DatasetLogResource[CV](datasetId: DatasetId,
                                  copyNumber: Long,
                                  fetchLog: (DatasetId, Long) => (Iterator[Delogger.LogEvent[CV]] => Unit) => Unit,
                                  formatDatasetId: DatasetId => String) extends ErrorHandlingSodaResource(formatDatasetId) {
  override def get = { (req: HttpRequest) => (resp: HttpServletResponse) =>
    try {
      fetchLog(datasetId, copyNumber) { it =>
        val r = OK ~> Write("application/json") { w =>
          ArrayIteratorEncode.toText(it.map(_.toString)).foreach(w.write)
        }
        r(resp)
      }
    } catch {
      case _: MissingVersion =>
        (OK ~> Json(JArray.canonicalEmpty))(resp)
    }
  }
}
