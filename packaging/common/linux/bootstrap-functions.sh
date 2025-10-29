#!/bin/sh

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

. ${LIBS_DIR}/@SETUP_JAVA_FILE_NAME@

# used by rpm, deb, zip and docker distributions
COMMON_JAVA_OPTS="
    @ADD_OPENS@ \
    -Dio.netty.tryReflectionSetAccessible=true \
    -Dfile.encoding=UTF-8 \
    -XX:+HeapDumpOnOutOfMemoryError \
    -XX:+ExitOnOutOfMemoryError"

LOGGING_JAVA_OPTS="
    -Djava.util.logging.config.file=${CONF_DIR}/ignite.java.util.logging.properties \
    -XX:HeapDumpPath=${LOG_DIR} \
    -Xlog:gc=info:file=${LOG_DIR}/${JVM_GC_LOG_NAME}::filecount=${JVM_GC_NUM_LOGS},filesize=${JVM_GC_LOG_SIZE}"

JAVA_MEMORY_OPTIONS="-Xmx${JVM_MAX_MEM} -Xms${JVM_MIN_MEM}"

JAVA_GC_OPTIONS="-XX:+Use${JVM_GC} -XX:G1HeapRegionSize=${JVM_G1HeapRegionSize}"

while IFS= read -r JAR || [ -n "${JAR}" ]; do
    if [ -z "${CLASSPATH+x}" ]; then
        CLASSPATH="-classpath ${LIBS_DIR}/${JAR}"
    else
        CLASSPATH="$CLASSPATH:${LIBS_DIR}/${JAR}"
    fi
done < "${LIBS_DIR}/@CLASS_PATH_FILE@"

if [ -z "${CLASSPATH+x}" ]; then
  echo "CLASSPATH could not be built from ${LIBS_DIR}/@CLASS_PATH_FILE@ file"
  exit 1
fi

MAIN_CLASS="@MAIN_CLASS@"

export JAVA_CMD_WITH_ARGS="${JAVACMD} \
  ${COMMON_JAVA_OPTS} \
  ${LOGGING_JAVA_OPTS} \
  ${JAVA_MEMORY_OPTIONS} \
  ${JAVA_GC_OPTIONS} \
  ${IGNITE3_EXTRA_JVM_ARGS} \
  ${CLASSPATH} \
  ${MAIN_CLASS}"

export APPLICATION_ARGS="\
  --config-path ${CONFIG_FILE} \
  --work-dir ${WORK_DIR} \
  --node-name ${NODE_NAME}"
