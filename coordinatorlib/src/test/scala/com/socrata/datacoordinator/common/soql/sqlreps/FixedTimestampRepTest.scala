package com.socrata.datacoordinator.common.soql.sqlreps

import java.time._

import org.scalatest.FunSuite
import org.scalatest.MustMatchers

class FixedTimestampRepTest extends FunSuite with MustMatchers {
  test("parsing extreme dates") {
    FixedTimestampRep.parse("1010101-02-03 04:05:06.070707+08:09:10") must equal (
      LocalDateTime.of(LocalDate.of(1010101, 2, 3),
                       LocalTime.of(4, 5, 6, 70707000)).
        atOffset(ZoneOffset.ofHoursMinutesSeconds(8, 9, 10)).
        toInstant
    )
  }
}
