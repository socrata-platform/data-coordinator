To move a dataset from one truth to another:

1- in data-coordinator run `sbt datasetMover/assembly` to build the dataset-mover subproject

2- create the config file according to the template below

3- ssh into a node with java that truth will accept connections from e.g. a core node (in the same environment as the truth)

4- run the following command for a dry run `java -Dconfig.file=<config-file> -jar <jar-file> <dataset_system_id> <truth to be moved to>`

to get the dataset_system_id run the following query on soda-fountain: `select dataset_system_id from datasets where resource_name = '_<fxf>';`

5- for the actual execution run `SOCRATA_COMMIT_MOVE=1 java -Dconfig.file=<config-file> -jar <jar-file> <dataset_system_id> <truth to be moved to>`

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
