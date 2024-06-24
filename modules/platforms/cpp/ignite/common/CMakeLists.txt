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

project(ignite-common)

set(TARGET ${PROJECT_NAME})

set(PUBLIC_HEADERS
    big_decimal.h
    big_integer.h
    bit_array.h
    bytes.h
    bytes_view.h
    detail/bits.h
    detail/config.h
    end_point.h
    error_codes.h
    ignite_date.h
    ignite_date_time.h
    ignite_duration.h
    ignite_error.h
    ignite_period.h
    ignite_result.h
    ignite_time.h
    ignite_timestamp.h
    ignite_type.h
    primitive.h
    uuid.h
)

set(SOURCES
    big_decimal.cpp
    big_integer.cpp
    detail/mpi.cpp
)

add_library(${TARGET} STATIC ${SOURCES})

target_link_libraries(${TARGET} PUBLIC MbedTLS::mbedtls)

target_include_directories(${TARGET} INTERFACE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}>
    $<INSTALL_INTERFACE:${IGNITE_INCLUDEDIR}>
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

if (${INSTALL_IGNITE_FILES})
    ignite_install_headers(FILES ${PUBLIC_HEADERS} DESTINATION ${IGNITE_INCLUDEDIR}/common)
endif()

ignite_test(bits_test DISCOVER SOURCES detail/bits_test.cpp LIBS ${TARGET})
ignite_test(bytes_test DISCOVER SOURCES bytes_test.cpp LIBS ${TARGET})
ignite_test(uuid_test DISCOVER SOURCES uuid_test.cpp LIBS ${TARGET})
ignite_test(bignum_test DISCOVER SOURCES bignum_test.cpp LIBS ${TARGET})
ignite_test(bit_array_test DISCOVER SOURCES bit_array_test.cpp LIBS ${TARGET})
ignite_test(primitive_test DISCOVER SOURCES primitive_test.cpp LIBS ${TARGET})
