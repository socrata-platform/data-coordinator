package com.socrata.datacoordinator.common.soql.sqlreps;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class FixedTimestampHelper {
    // Postgresql will hand dates to us formatted like
    //   YYYY-MM-DD HH:MM:SS[.ssss...]±HH[:MM[:SS]][ BC]
    // This regex pulls those bits out and then
    // we'll turn them into an Instant.
    private static final Pattern PATTERN = Pattern.compile("^([0-9]{4}-[0-9]{2}-[0-9]{2}) ([0-9]{2}:[0-9]{2}:[0-9]{2}(?:.[0-9]+)?)([+-][0-9]{2}(?::[0-9]{2}(?::[0-9]{2})?)?)( BC)?$");

    public static final OffsetDateTime parse(String s) {
        Matcher m = PATTERN.matcher(s);
        if(!m.matches()) throw new IllegalArgumentException("Malformed timestamp");

        String date = m.group(1);
        String time = m.group(2);
        String offset = m.group(3);
        boolean bc = m.group(4) != null;

        return LocalDateTime.of(LocalDate.parse((bc ? "-" : "") + date),
                                LocalTime.parse(time)).
            atOffset(ZoneOffset.of(offset));
    }
}
