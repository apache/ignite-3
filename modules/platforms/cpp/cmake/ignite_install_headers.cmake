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

# ignite_install_headers(FILES <header>... DESTINATION <dest> COMPONENT <component>)
#
# Function to install header files.
function(ignite_install_headers)
    cmake_parse_arguments(IGNITE_INSTALL "" "DESTINATION" "FILES;COMPONENT" ${ARGN})

    foreach(HEADER ${IGNITE_INSTALL_FILES})
        get_filename_component(SUBDIR ${HEADER} DIRECTORY)
        install(FILES ${HEADER} DESTINATION ${IGNITE_INSTALL_DESTINATION}/${SUBDIR} COMPONENT ${IGNITE_INSTALL_COMPONENT})
    endforeach()
endfunction()
