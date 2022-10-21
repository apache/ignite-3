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

# LOAD VARIABLES
if [ -f "@ENV_FILE@" ]; then
  . "@ENV_FILE@"
fi

LOG_DIR="@LOG_DIR@"
JAVA_OPTS="@JAVA_OPTS@"
ADDITIONAL_OPTS="@ADDITIONAL_OPTS@"
CLASSPATH="@LIB_DIR@/@APP_JAR@:@LIB_DIR@/*"
MAIN_CLASS="@MAIN_CLASS@"
ARGS="@ARGS@"

DEFAULT_JVM_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED \
--add-opens java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens java.base/java.io=ALL-UNNAMED \
--add-opens java.base/java.nio=ALL-UNNAMED \
--add-opens java.base/java.math=ALL-UNNAMED \
--add-opens java.base/java.util=ALL-UNNAMED \
--add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
-Dfile.encoding=UTF-8 \
-XX:+HeapDumpOnOutOfMemoryError \
-XX:+ExitOnOutOfMemoryError \
-XX:HeapDumpPath=$LOG_DIR"


warn () {
    echo "$*"
} >&2

die () {
    echo
    echo "$*"
    echo
    exit 1
} >&2

# Determine the Java command to use to start the JVM.
if [ -n "$JAVA_HOME" ] ; then
    if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
        # IBM's JDK on AIX uses strange locations for the executables
        JAVACMD=$JAVA_HOME/jre/sh/java
    else
        JAVACMD=$JAVA_HOME/bin/java
    fi
    if [ ! -x "$JAVACMD" ] ; then
        die "ERROR: JAVA_HOME is set to an invalid directory: $JAVA_HOME
        Please set the JAVA_HOME variable in your environment to match the
        location of your Java installation."
    fi
else
    JAVACMD=java
    which java >/dev/null 2>&1 || die "ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.
    Please set the JAVA_HOME variable in your environment to match the
    location of your Java installation."
fi


CMD="${JAVACMD} \
${DEFAULT_JVM_OPTS} \
${JAVA_OPTS} \
${ADDITIONAL_OPTS} \
-classpath ${CLASSPATH} \
${MAIN_CLASS} \
${ARGS}"

${CMD}

