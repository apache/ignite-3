#!/usr/bin/env bash
set -o nounset; set -o errexit; set -o pipefail; set -o errtrace; set -o functrace



## VARS ##
PACKAGE_VERSION="${1}"

RPM_WORK_DIR="/tmp/apache-ignite-rpm"



## START ##
mkdir -pv ${RPM_WORK_DIR}/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
cp -rfv ignite ${RPM_WORK_DIR}/BUILD/
cp -rfv apache-ignite.spec ${RPM_WORK_DIR}/SPECS/
sed -r "4 i if [ \"\$(whoami)\" != \"ignite\" ]; then echo \"Ignite CLI can only be run by 'ignite' user.\"; echo \"Swith user to ignite by executing 'su ignite'\"; exit 1; fi" \
    -i ${RPM_WORK_DIR}/BUILD/ignite
rpmbuild -vv \
         -bb \
         --define "_topdir ${RPM_WORK_DIR}" \
         ${RPM_WORK_DIR}/SPECS/apache-ignite.spec
cp -rfv ${RPM_WORK_DIR}/RPMS/noarch/apache-ignite-${PACKAGE_VERSION}.noarch.rpm ./

