**data-coordinator** is a set of related services, libraries, and scripts that takes SoQL upserts, inserts them to the truth store, watches truth store logs and writes the data to the secondary stores.

## Projects

* coordinator - contains the REST service for the data coordinator, as well as services for secondary watchers, various scripts including backup
* coordinatorlib
    - common utilities and data structures for working with truth store and secondaries, at a lower level
    - database migrations are in src/main/resources
* coordinatorlib-soql - en/decoders between SoQL and JSON, CSV, and SQL
* dummy-secondary - a dummy secondary store implementation for testing only
* secondarylib - trait for Secondary store

## Running

To run the tests, from the SBT shell:

    project data-coordinator
    test:test

To run the data coordinator, from the regular Linux/OSX shell prompt:

    bin/start_dc.sh

The above scripts builds the assembly if its not present and runs the fat jar on the command line, which is much more memory efficient than running it from sbt.  If you need to force a rebuild, simply run `sbt clean` beforehand.

### Migrations

To run migrations in this project from SBT:
```sh
sbt -Dconfig.file=/etc/soda2.conf "coordinator/run-main com.socrata.datacoordinator.primary.MigrateSchema migrate"
```
Alternatively, to build from scratch and run migrations:
```sh
sbt clean
bin/run_migrations.sh
```

To run migrations without building from scratch: `bin/run_migrations.sh`

The command is one of `migrate`, `undo`, `redo`, and there is a second optional paramter for undo for the number of changes to roll back.

Running from sbt is recommended in a development environment because
it ensures you are running the latest migrations without having to build a 
new assembly.
