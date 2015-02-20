FROM socrata/java

# TODO expose JMX
EXPOSE 6030 6031

ENV SERVER_ROOT /srv/query-coordinator/
# TODO sbt task to copy latest jar into docker/
ENV SERVER_ARTIFACT query-coordinator-assembly.jar
ENV SERVER_CONFIG query-coordinator.conf

# defaults
ENV JAVA_XMX 512m
ENV LOG_METRICS false
ENV ENABLE_GRAPHITE false
ENV GRAPHITE_HOST 0.0.0.0
ENV GRAPHITE_PORT 0

WORKDIR $SERVER_ROOT

COPY ship.d /etc/ship.d
COPY ${SERVER_CONFIG}.j2 $SERVER_ROOT/
COPY $SERVER_ARTIFACT $SERVER_ROOT/
