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

. @LIB_DIR@/@SETUP_JAVA_FILE_NAME@

LOG_DIR="@LOG_DIR@"
CLASSPATH="@LIB_DIR@/@APP_JAR@:@LIB_DIR@/*"
MAIN_CLASS="@MAIN_CLASS@"

DEFAULT_JVM_OPTS="-Dfile.encoding=UTF-8 \
    --add-opens=java.base/java.lang=ALL-UNNAMED \
    -XX:+HeapDumpOnOutOfMemoryError \
    -XX:+ExitOnOutOfMemoryError \
    -XX:HeapDumpPath=${LOG_DIR}"

${JAVACMD} ${DEFAULT_JVM_OPTS} ${IGNITE3_OPTS} -classpath ${CLASSPATH} ${MAIN_CLASS} "$@"
