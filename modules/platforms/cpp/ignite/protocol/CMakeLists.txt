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

set(TARGET ignite-protocol)

set(SOURCES
    sql/column_meta.cpp sql/column_meta.h
    bitset_span.h
    buffer_adapter.cpp buffer_adapter.h
    extension_types.h
    heartbeat_timeout.h
    messages.cpp messages.h
    protocol_context.h
    protocol_version.cpp protocol_version.h
    reader.cpp reader.h
    utils.cpp utils.h
    writer.cpp writer.h
)

add_library(${TARGET} OBJECT ${SOURCES})
target_link_libraries(${TARGET} PUBLIC ignite-common msgpack-c)

set_target_properties(${TARGET} PROPERTIES VERSION ${CMAKE_PROJECT_VERSION})
set_target_properties(${TARGET} PROPERTIES POSITION_INDEPENDENT_CODE 1)
