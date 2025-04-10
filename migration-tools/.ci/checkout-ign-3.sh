#!/bin/bash

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

GRADLE_PROPS_FILE="$(mktemp)"
./gradlew :properties > $GRADLE_PROPS_FILE

ignite3_vendor="$(awk -F ': ' '/ignite3.vendor/ {print $2}' $GRADLE_PROPS_FILE)"
echo "Ignite 3 Vendor: $ignite3_vendor"

commit_hash="$(awk -F ': ' '/ignite3.version.commit/ {print $2}' $GRADLE_PROPS_FILE)"
echo "Ignite 3 Commit Hash: $commit_hash"

ignite3_version="$(awk -F ': ' '/ignite3.version/ {print $2}' $GRADLE_PROPS_FILE)"
echo "Ignite 3 version: $ignite3_version"

ignite3_docker_version="$(awk -F ': ' '/ignite3.docker.version/ {print $2}' $GRADLE_PROPS_FILE)"
echo "Ignite 3 Docker version: $ignite3_docker_version"

[[ $ignite3_vendor == org.gridgain* ]] \
    && root_dir="$VCSROOT__GG9" \
    || root_dir="$VCSROOT__IGNITE3"

[[ $ignite3_vendor == org.gridgain* ]] \
    && docker_build_extra_args="-x cmakeBuildMacAarch64" \
    || docker_build_extra_args=""

echo "##teamcity[setParameter name='IGNITE_3_PROJECT_FOLDER' value='${root_dir}']"
echo "##teamcity[setParameter name='IGNITE_3_DOCKER_VERSION' value='${ignite3_docker_version}']"
echo "##teamcity[setParameter name='IGNITE_3_VENDOR' value='${ignite3_vendor}']"
echo "##teamcity[setParameter name='IGNITE_3_VERSION' value='${ignite3_version}']"
echo "##teamcity[setParameter name='IGNITE_3_DOCKER_BUILD_EXTRA_ARGS' value='${docker_build_extra_args}']"
echo "Moving to: $root_dir"
cd "$root_dir"

git fetch --all
git -c advice.detachedHead=false checkout $commit_hash
