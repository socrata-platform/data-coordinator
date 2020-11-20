package com.socrata.datacoordinator.common.soql.sqlreps;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

class FixedTimestampHelper {
    public static Instant parse(String s) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(s, Instant::from);
    }
}
