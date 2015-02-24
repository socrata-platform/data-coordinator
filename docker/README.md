# Docker support

The files in this directory allow you to build a docker image.  The query coordinator assembly must be 
copied to `query-coordinator-assembly.jar` in this directory before building.

## Required Runtime Variables

* `ZOOKEEPER_ENSEMBLE` - The zookeeper cluster to talk to, in the form of `["10.0.0.1:2181", "10.0.0.2:2818"]`
* `SECONDARY_INSTANCES` - The list of all the secondary instances that this query coordinator should talk to.

## Optional Runtime Variables

See the Dockerfile for defaults.

* `ARK_HOST` - The IP address of the host of the docker container, used for service advertisements.
* `JAVA_XMX` - Sets the -Xmx and -Xms parameters to control the JVM heap size
* `LOG_METRICS` - Should various metrics information be logged to the log
* `ENABLE_GRAPHITE` - Should various metrics information be reported to graphite
* `GRAPHITE_HOST` - The hostname or IP of the graphite server, if enabled
* `GRAPHITE_PORT` - The port number for the graphite server, if enabled
