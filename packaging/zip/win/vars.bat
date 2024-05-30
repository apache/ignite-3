@rem
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

set NODE_NAME=defaultNode

set WORK_DIR=%IGNITE_HOME%\work
set LOG_DIR=%IGNITE_HOME%\log
set LIBS_DIR=%IGNITE_HOME%\lib
set CONF_DIR=%IGNITE_HOME%\etc
set INSTALL_DIR=%IGNITE_HOME%

set LOG_FILE_PATTERN=%LOG_DIR%\ignite3db-%g.log
set CONFIG_FILE=%CONF_DIR%\ignite-config.conf

@rem JVM props
set JVM_MAX_MEM=16384m
set JVM_MIN_MEM=16384m
set JVM_GC=G1GC
set JVM_G1HeapRegionSize=32M
set JVM_GC_LOG_NAME=gc.log
set JVM_GC_LOG_SIZE=104857600
set JVM_GC_NUM_LOGS=10

@rem For any additional users settings
set IGNITE3_EXTRA_JVM_ARGS=
