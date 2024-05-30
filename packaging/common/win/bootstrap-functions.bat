@rem Licensed to the Apache Software Foundation (ASF) under one or more
@rem contributor license agreements. See the NOTICE file distributed with
@rem this work for additional information regarding copyright ownership.
@rem The ASF licenses this file to You under the Apache License, Version 2.0
@rem (the "License"); you may not use this file except in compliance with
@rem the License. You may obtain a copy of the License at
@rem
@rem      http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.
@rem

set COMMON_JAVA_OPTS=@ADD_OPENS@ ^
-Dio.netty.tryReflectionSetAccessible=true ^
-Dfile.encoding=UTF-8 ^
-XX:+HeapDumpOnOutOfMemoryError ^
-XX:+ExitOnOutOfMemoryError

set LOGGING_JAVA_OPTS=-Djava.util.logging.config.file="@CONF_DIR@\ignite.java.util.logging.properties" ^
-XX:HeapDumpPath="@LOG_DIR@" ^
-Xlog:gc=info:file="@LOG_DIR@\%JVM_GC_LOG_NAME%"::filecount=%JVM_GC_NUM_LOGS%,filesize=%JVM_GC_LOG_SIZE%

set CLASSPATH=-classpath "@INSTALL_DIR@\lib\*" @MAIN_CLASS@

set JAVA_CMD_WITH_ARGS="%JAVA_EXE%" %COMMON_JAVA_OPTS% %LOGGING_JAVA_OPTS% %IGNITE3_EXTRA_JVM_ARGS% %CLASSPATH%

set APPLICATION_ARGS=--config-path "%CONFIG_FILE%" --work-dir "%WORK_DIR%" --node-name %NODE_NAME%
