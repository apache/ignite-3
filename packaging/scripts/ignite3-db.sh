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
  if [ -z ${IGNITE_HOME+x} ]; then IGNITE_HOME=$(pwd); fi

  . $IGNITE_HOME/etc/bootstrap-config

  # Export these variables so that IgniteCliRunner can use them as default values
  export IGNITE_NODE_NAME=$NODE_NAME
  export IGNITE_WORK_DIR=$WORK_PATH
  export IGNITE_CONFIG_PATH=$IGNITE_CONFIG_FILE


start() {

  ############# STAGE 2: BUILD CMD ###############

  # FORM A START COMMAND
  CMD="java \
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
  -classpath $LIBS_PATH/ignite-runner.jar:$LIBS_PATH/* org.apache.ignite.app.IgniteCliRunner"

  ############# STAGE 3: RUN CMD, REPORT ###############
  exec ${CMD} >>${LOG_OUT_FILE:-/dev/null} 2>&1 < /dev/null & jobs -p > ${IGNITE_HOME}/pid

  # TODO: SAVE PROCESS PID AND LINK IT WITH THE NODE NAME
  # TODO: WAIT FOR NODE IS STARTED
  # TODO: REPORT SUCCESS OR FAILURE
  # TODO: SHOW HOW TO CONNECT TO THE STARTED NODE
}

stop() {
  kill -9 "$(cat ${IGNITE_HOME}/pid)"
}

case $1 in
	start)
    start
		;;
	stop)
    stop
		;;
	restart)
		stop
		start
		;;
	*)
		echo "Unknown command '$1', available commands: start, stop, restart"
		exit 1
		;;
  esac
