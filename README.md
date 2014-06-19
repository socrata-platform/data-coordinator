## query-coordinator

An HTTP/REST microservice that analyzes SoQL queries and dispatches them to an execution engine.

## Running

Tests:

    sbt test

Query Coordinator service:

    bin/start_qc.sh

The above script will build an assembly if it is not present already.

If you are actively developing QC, it is probably better to start the QC from the SBT shell so you always have the latest copy of the code -- `sbt run`.