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
    test:test it:test

To run the data coordinator, from the regular Linux/OSX shell prompt:

    bin/start_dc.sh

The above scripts builds the assembly if its not present and runs the fat jar on the command line, which is much more memory efficient than running it from sbt.  If you need to force a rebuild, simply run `sbt clean` beforehand.

### Secondary watcher

If you haven't already, edit your `/etc/soda2.conf` file to remove the contents of the `com.socrata.coordinator.common.secondary.instances` section. It should resemble something like this:

```
com.socrata.coordinator.common = {
  database = ${common-database} {
    app-name = "data coordinator"
    database = "datacoordinator"
  }
  instance = ${data-coordinator-instance}
  secondary {
    ...
    instances {}
    ...
  }
```

From within the `soql-postgres-adapter` repository, start secondary watcher like this:

```sh
sbt clean assembly
java -Djava.net.preferIPv4Stack=true -Dconfig.file=/etc/pg-secondary.conf -jar store-pg/target/scala-2.10/store-pg-assembly-3.1.4-SNAPSHOT.jar
```

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

## Notes

Below is a copy of the email distributed to engineering when breaking changes were made to the secondary watcher architecture:

>Hi All,
>The secondary architecture has been inverted (thank you @robert.macomber).
>The secondaries (`pg`, `spandex`, `geocoding`) are no longer dynamically 
>loaded as jar files in `secondary-watcher`. But, instead are now their own
>executable and `secondary-watcher` is now a library that they use.

>The install and start scripts in `docs/onramp` have been updated (pending merge) -- they also now include the geocoding / region coding secondary.

>How to update your stack:
> - Pull master of `data-coordinator`, `soql-postgres-adapter`, and `geocoding-secondary` (if you wish)
> - Fetch `spandex` and `docs` and check-out branches `en-7807` and `aerust/en-7807` respectively (branches aren't quite merged but functioning).
> - Run `sbt assembly` for all of the above scala projects.
> - Update your `/etc/soda2.conf` file; `com.socrata.coordinator.common.secondary.instances` should be empty (but still needs to be there :( ). Copy the new config files for the secondaries over to `/etc`:

>```sh
>sudo cp $DEV_DIR/docs/onramp/services/pg-secondary.conf /etc/
>sudo cp $DEV_DIR/docs/onramp/services/spandex-secondary.conf /etc/
>sudo cp $DEV_DIR/docs/onramp/services/geocoding-secondary.conf /etc/
>```

>   If you want to use the geocoding secondary you will need to add a MapQuest app-token to the config and add it to the
>      `secondary_stores_config` table in `datacoordinator` (truth).
>       
>       INSERT INTO secondary_stores_config (store_id, next_run_time, interval_in_seconds, is_feedback_secondary) VALUES( 'geocoding', now(), 5, true);
       
> - You can now run the secondary stores as their own executable. (See `docs/onramp/start.sh for specifics)
> - You can delete your `~/secondary-stores` directory :tada:

>Thanks, Alexa
