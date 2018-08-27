package com.socrata.datacoordinator.secondary.feedback

import com.rojoma.json.v3.ast.JObject
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId, StrategyType}
import com.socrata.datacoordinator.secondary.ComputationStrategyInfo
import org.scalatest.{FunSuite, ShouldMatchers}

class CookieOperatorTest extends FunSuite with ShouldMatchers {

  val id = new UserColumnId(":id")
  val name = new UserColumnId("name")
  val address = new UserColumnId("address")
  val state = new UserColumnId("state")
  val postalCode = new UserColumnId("postal_code")
  val location = new UserColumnId("location")
  val postalCodeLocation = new UserColumnId("postal_code_location")

  val region = new UserColumnId("region")

  val columns = Map(
    id -> new ColumnId(1),
    name -> new ColumnId(2),
    address -> new ColumnId(3),
    state -> new ColumnId(4),
    postalCode -> new ColumnId(5),
    location -> new ColumnId(6),
    postalCodeLocation -> new ColumnId(7)
  )

  val columnsWithRegion = columns ++ Map(region -> new ColumnId(8))

  val computedColumns = Map(
    location -> ComputationStrategyInfo(StrategyType("geocoding"), Seq(address, state, postalCode), JObject(Map())),
    postalCodeLocation -> ComputationStrategyInfo(StrategyType("geocoding"), Seq(postalCode), JObject(Map()))
  )

  val computedColumnsWithRegion = computedColumns ++ Map(
    region -> ComputationStrategyInfo(StrategyType("georegion_match_on_point"), Seq(location), JObject(Map())))

  val cookie = CookieSchema(
    dataVersion = DataVersion(100),
    copyNumber = CopyNumber(5),
    systemId = new UserColumnId(":id"),
    columnIdMap = columns,
    strategyMap = computedColumns,
    obfuscationKey = Array(),
    computationRetriesLeft = 5,
    dataCoordinatorRetriesLeft = 5,
    resync = false
  )

  val cookieWithRegion = cookie.copy(columnIdMap = columnsWithRegion, strategyMap = computedColumnsWithRegion)

  def testDeleteColumns(columns: Set[UserColumnId],
                        cookie: CookieSchema,
                        expectedDeleted: Set[UserColumnId],
                        expectedReliant: Set[UserColumnId]): Unit = {
    val (deleted, reliant, newCookie) = CookieOperator.deleteColumns(columns, cookie)

    val expectedNewCookie = cookie.copy(
      columnIdMap = cookie.columnIdMap.filterKeys(!expectedDeleted(_)),
      strategyMap = cookie.strategyMap.filterKeys(!expectedDeleted(_))
    )

    deleted should equal(expectedDeleted)
    reliant should equal(expectedReliant)
    newCookie should equal(expectedNewCookie)
  }

  test("deletes an unreferenced column") {
    testDeleteColumns(Set(name), cookie, Set(name), Set.empty)
  }

  test("deletes a source column") {
    testDeleteColumns(Set(address), cookie, Set(address, location), Set(location))
  }

  test("deletes a computed column") {
    testDeleteColumns(Set(location), cookie, Set(location), Set(location))
  }

  test("deletes a source column with multiple computed columns") {
    testDeleteColumns(Set(postalCode), cookie, Set(postalCode, location, postalCodeLocation), Set(location, postalCodeLocation))
  }

  test("deletes multiple columns") {
    testDeleteColumns(Set(address, postalCode, location), cookie, Set(address, postalCode, location, postalCodeLocation), Set(location, postalCodeLocation))
  }

  test("deletes chained computed columns") {
    testDeleteColumns(Set(address), cookieWithRegion, Set(address, location, region), Set(location, region))
  }
}
