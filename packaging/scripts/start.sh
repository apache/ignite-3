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

############# STAGE 1: SETUP ENV ###############

# LOAD VARIABLES
. $IGNITE_HOME/config/bootstrap-config



############# STAGE 2: BUILD CMD ###############

# FORM A START COMMAND
CMD="$JAVA_HOME/bin/java \
--add-opens java.base/java.lang=ALL-UNNAMED \
--add-opens java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens java.base/java.io=ALL-UNNAMED \
--add-opens java.base/java.nio=ALL-UNNAMED \
--add-opens java.base/java.math=ALL-UNNAMED \
--add-opens java.base/java.util=ALL-UNNAMED \
--add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
-Dio.netty.tryReflectionSetAccessible=true \
-Djava.util.logging.config.file=$CONFIG_PATH/ignite.java.util.logging.properties \
-classpath $LIBS_PATH/ignite-runner.jar:$LIBS_PATH/* org.apache.ignite.app.IgniteCliRunner \
--config-path $IGNITE_CONFIG_FILE \
--work-dir $WORK_PATH \
$NODE_NAME"



############# STAGE 3: RUN CMD, REPORT ###############

# RUN CMD
exec ${CMD} >>${LOG_OUT_FILE:-/dev/null} 2>&1 < /dev/null & jobs -p > pid

# TODO: SAVE PROCESS PID AND LINK IT WITH THE NODE NAME

# TODO: WAIT FOR NODE IS STARTED

# TODO: REPORT SUCCESS OR FAILURE

# TODO: SHOW HOW TO CONNECT TO THE STARTED NODE

