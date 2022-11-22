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
if [ -n "${JAVA_HOME}" ] ; then
    if [ -x "${JAVA_HOME}/jre/sh/java" ] ; then
        # IBM's JDK on AIX uses strange locations for the executables
        export JAVACMD=${JAVA_HOME}/jre/sh/java
    else
        export JAVACMD=${JAVA_HOME}/bin/java
    fi
    if [ ! -x "${JAVACMD}" ] ; then
        die "ERROR: JAVA_HOME is set to an invalid directory: ${JAVA_HOME}
        Please set the JAVA_HOME variable in your environment to match the
        location of your Java installation."
    fi
else
    export JAVACMD=java
    which java >/dev/null 2>&1 || die "ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.
    Please set the JAVA_HOME variable in your environment to match the
    location of your Java installation."
fi
