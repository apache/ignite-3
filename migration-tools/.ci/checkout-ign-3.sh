#!/bin/bash

#
# Copyright (C) GridGain Systems. All Rights Reserved.
# _________        _____ __________________        _____
# __  ____/___________(_)______  /__  ____/______ ____(_)_______
# _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
# / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
# \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
