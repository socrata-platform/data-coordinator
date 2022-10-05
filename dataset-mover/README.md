Here is a sample dataset-mover configuration file actual connection
information removed (search for `XXXX` to find all the bits that need
to be filled in)

```
common-database {
  port = 5432
  app-name = "dataset-mover"
}

common-truth = ${common-database} {
  username = "XXXX"
  password = "XXXX"
  database = "XXXX"
}

common-secondary = ${common-database} {
  username = "XXXX"
  password = "XXXX"
  database = "XXXX"
}

com.socrata.coordinator.datasetmover {
  truths = {
    "alpha-XXXX": ${common-truth} {
      host = "XXXX"
    }
    "bravo-XXXX": ${common-truth} {
      host = "XXXX"
    }
  }

  pg-secondaries = {
    "pg1-XXXX": ${common-secondary} {
      host = "XXXX"
    }
    "pg2-XXXX": ${common-secondary} {
      host = "XXXX"
    }
    "pg3-XXXX": ${common-secondary} {
      host = "XXXX"
    }
  }

  additional-acceptable-secondaries = [ "geocoding" ]

  soda-fountain = ${common-database} {
    host = "XXXX"
    database = "XXXX"
    username = "XXXX"
    password = "XXXX"
  }

  log4j = {
    rootLogger = [ INFO, console ]
    appender {
      console.class = org.apache.log4j.ConsoleAppender
      console.props {
        layout.class = org.apache.log4j.PatternLayout
        layout.props {
          ConversionPattern = "[%t] %p %c{1} %d %m%n"
        }
      }
    }
  }

  tablespace = "XXXX" -- see the DC configuration for this value

  write-lock-timeout = 5s

  post-soda-fountain-update-pause = 15s
}
```
