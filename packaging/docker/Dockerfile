FROM eclipse-temurin:11 AS jre-build

RUN $JAVA_HOME/bin/jlink \
         --add-modules ALL-MODULE-PATH \
         --strip-debug \
         --no-man-pages \
         --no-header-files \
         --compress=2 \
         --output /javaruntime

# Same base image as eclipse-temurin
FROM ubuntu:22.04

ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"
COPY --from=jre-build /javaruntime $JAVA_HOME

# Set default memory and GC configuration
ENV JVM_MAX_MEM="16g"
ENV JVM_MIN_MEM="16g"

ENV JVM_GC="G1GC"
ENV JVM_G1HeapRegionSize="32M"

ENV IGNITE3_EXTRA_JVM_ARGS=""

# Copy and setup DB app
ENV IGNITE_HOME=/opt/ignite

ENV LIBS_DIR=$IGNITE_HOME/lib

ENV IGNITE_NODE_NAME=defaultNode
ENV IGNITE_WORK_DIR=$IGNITE_HOME/work
ENV IGNITE_CONFIG_PATH=$IGNITE_HOME/etc/ignite-config.conf

COPY dist/db $IGNITE_HOME

EXPOSE 3344 10300 10800

# Copy CLI app
ENV IGNITE_CLI_HOME=/opt/ignite3cli

COPY dist/cli $IGNITE_CLI_HOME

# Copy entrypoint script
COPY dist/docker-entrypoint.sh /usr/local/bin/

# Make sure the locale is UTF-8 so that CLI prints tables correctly
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

ENTRYPOINT ["docker-entrypoint.sh"]
