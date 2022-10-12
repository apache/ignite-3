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

. @CONF_DIR@/@VARS_FILE_NAME@

CMD="java \
--add-opens java.base/java.lang=ALL-UNNAMED \
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
-XX:HeapDumpPath=@LOG_DIR@ \
-Xlog:gc=info:file=@LOG_DIR@/${JVM_GC_LOG_NAME}::filecount=${JVM_GC_NUM_LOGS},filesize=${JVM_GC_LOG_SIZE} \
-Dio.netty.tryReflectionSetAccessible=true \
-Djava.util.logging.config.file=@CONF_DIR@/ignite.java.util.logging.properties \
${IGNITE3_EXTRA_JVM_ARGS} \
-classpath @INSTALL_DIR@/lib:@INSTALL_DIR@/lib/* org.apache.ignite.app.IgniteCliRunner \
--config-path ${CONFIG_FILE} \
--work-dir ${WORK_DIR} \
--node-name ${NODE_NAME}"

${CMD}
