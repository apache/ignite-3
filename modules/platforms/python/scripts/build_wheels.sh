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
PY_VERS="cp39 cp310 cp311 cp312 cp313"

function repair_wheel {
    wheel="$1"
    if ! auditwheel show "$wheel"; then
        echo "Skipping non-platform wheel $wheel"
    else
        auditwheel repair "$wheel" --plat "$PLAT" -w /wheels
    fi
}

for PY_VER in $PY_VERS; do
    for PYBIN in /opt/python/*/bin; do
        if [[ $PYBIN =~ ^(.*)$PY_VER/(.*)$ ]]; then
            echo -e "\e[32m >>> \e[0m"
            echo -e "\e[32m >>> Preparing a wheel for Python $PYBIN \e[0m"
            echo -e "\e[32m >>> \e[0m"

            # Compile wheels
            "${PYBIN}/pip" wheel /$PACKAGE_NAME/ --no-deps -w /wheels

            # Bundle external shared libraries into the wheels
            for whl in /wheels/*.whl; do
                if [[ $whl =~ ^(.*)$PY_VER-(.*)$ ]] && [[ ! $whl =~ ^(.*)manylinux(.*)$ ]]; then
                    "${PYBIN}/pip" wheel /$PACKAGE_NAME/ --no-deps -w /wheels
                    repair_wheel "$whl"
                fi
            done
        fi
    done
done

for whl in /wheels/*.whl; do
    if [[ ! $whl =~ ^(.*)manylinux(.*)$ ]]; then
        rm "$whl"
    fi
done

chown -R `stat -c "%u:%g" /wheels` /wheels/*

rm -rf /$PACKAGE_NAME/*.egg-info
rm -rf /$PACKAGE_NAME/.eggs
