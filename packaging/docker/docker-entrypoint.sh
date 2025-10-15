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

if [ "$1" = 'cli' ]; then
  shift
  # Override user.home property to keep CLI-produced files in the separate work directory
  IGNITE3_OPTS=-Duser.home=$IGNITE_CLI_WORK_DIR exec sh "$IGNITE_CLI_HOME"/bin/ignite3 "$@"
fi

. @LIB_DIR@/@BOOTSTRAP_FILE_NAME@

LOGGING_JAVA_OPTS="-Djava.util.logging.config.file=@CONF_DIR@/ignite.java.util.logging.properties"

JAVA_MEMORY_OPTIONS="-Xmx${JVM_MAX_MEM} -Xms${JVM_MIN_MEM}"

JAVA_GC_OPTIONS="-XX:+Use${JVM_GC} -XX:G1HeapRegionSize=${JVM_G1HeapRegionSize}"

CMD="${JAVACMD} \
  ${COMMON_JAVA_OPTS} \
  ${LOGGING_JAVA_OPTS} \
  ${JAVA_MEMORY_OPTIONS} \
  ${JAVA_GC_OPTIONS} \
  ${IGNITE3_EXTRA_JVM_ARGS} \
  ${CLASSPATH}"

exec $CMD "$@"
