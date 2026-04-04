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

PY_VERS="3.10,3.11,3.12,3.13,3.14"
PACKAGE_NAME="pyignite_dbapi"
SRC_DIR="$(pwd)"
DISTR_DIR="$SRC_DIR/distr/"
CPP_DIR="$SRC_DIR/../cpp/"
DEFAULT_DOCKER_IMAGE="ignite_python_wheels_build"

usage() {
    cat <<EOF
create_distr.sh: creates wheels and source distr for different python versions and platforms.

Usage: ${0} [options]

The options are as follows:
-h|--help
    Display this help message.

-a|--arch
    Specify architecture, supported variants: i686,x86,x86_64. Build all supported by default.

-d|--dir
    Specify directory where to store artifacts. Default $(PWD)/../distr

EOF
    exit 0
}

normalize_path() {
    mkdir -p "$DISTR_DIR"
    cd "$DISTR_DIR" || exit 1
    DISTR_DIR="$(pwd)"
    cd "$CPP_DIR" || exit 1
    CPP_DIR="$(pwd)"
    cd "$SRC_DIR" || exit 1
    SRC_DIR="$(pwd)"
}

run_wheel_arch() {
    if [[ $1 =~ ^(x86_64)$ ]]; then
        PLAT="manylinux2014_x86_64"
        PRE_CMD=""
        DOCKER_IMAGE="$DEFAULT_DOCKER_IMAGE"
    else
        echo "unsupported architecture $1, only x86_64 supported"
        exit 1
    fi

    WHEEL_DIR="$DISTR_DIR"
    mkdir -p "$WHEEL_DIR"
    docker run --rm -e PLAT=$PLAT -v "$SRC_DIR":/$PACKAGE_NAME -v "$CPP_DIR":/cpp -v "$WHEEL_DIR":/dist $DOCKER_IMAGE $PRE_CMD /$PACKAGE_NAME/scripts/build_wheels.sh "$PY_VERS"
}

while [[ $# -ge 1 ]]; do
    case "$1" in
        -h|--help) usage;;
        -a|--arch) ARCH="$2"; shift 2;;
        -d|--dir) DISTR_DIR="$2"; shift 2;;
        *) break;;
    esac
done

normalize_path

docker run --rm -v "$SRC_DIR":/$PACKAGE_NAME -v "$CPP_DIR":/cpp -v "$DISTR_DIR":/dist $DEFAULT_DOCKER_IMAGE /$PACKAGE_NAME/scripts/create_sdist.sh "$PY_VERS"

docker build scripts/ -t ignite_python_wheels_build

if [[ -n "$ARCH" ]]; then
    run_wheel_arch "$ARCH"
else
    run_wheel_arch "x86_64"
fi
