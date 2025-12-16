#!/bin/bash
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

set -e -u -x

PACKAGE_NAME="pyignite_dbapi"
PY_VERS=$1

# Converting input from '3.10, 3.11...' to '310 311...'
PREPARED_VERS=$(echo $PY_VERS | sed -r 's/\.//g' | tr ',' ' ')


function repair_wheel {
    wheel="$1"
    if ! auditwheel show "$wheel"; then
        echo "Skipping non-platform wheel $wheel"
    else
        auditwheel repair "$wheel" --plat "$PLAT" -w /dist
    fi
}

for PY_VER in $PREPARED_VERS; do
    for PYBIN in /opt/python/*/bin; do
        if [[ $PYBIN =~ ^(.*)cp$PY_VER/(.*)$ ]]; then
            echo -e "\e[32m >>> \e[0m"
            echo -e "\e[32m >>> Preparing a wheel for Python $PY_VER ($PYBIN) \e[0m"
            echo -e "\e[32m >>> \e[0m"

            # Compile wheels
            "${PYBIN}/pip" wheel /$PACKAGE_NAME/ --no-deps -w /dist

            # Bundle external shared libraries into the wheels
            for whl in /dist/*.whl; do
                if [[ $whl =~ ^(.*)cp$PY_VER-(.*)$ ]] && [[ ! $whl =~ ^(.*)manylinux(.*)$ ]]; then
                    "${PYBIN}/pip" wheel /$PACKAGE_NAME/ --no-deps -w /dist
                    repair_wheel "$whl"
                fi
            done
        fi
    done
done

for whl in /dist/*.whl; do
    if [[ ! $whl =~ ^(.*)manylinux(.*)$ ]]; then
        rm "$whl"
    fi
done

chown -R `stat -c "%u:%g" /$PACKAGE_NAME` /$PACKAGE_NAME

rm -rf /$PACKAGE_NAME/*.egg-info
rm -rf /$PACKAGE_NAME/.eggs
