#!/usr/bin/env bash
set -o nounset; set -o errexit; set -o pipefail; set -o errtrace; set -o functrace


POMS=$(find . -name pom.xml)
xpath -e "project/properties/*" parent/pom.xml 2>&1 | \
  grep -E "^<.*\.version>" | \
  sed -r 's|<(.*)>.*<\/.*>|\1|' | \
  while read -r property; do
    FOUND=false
    for pom in ${POMS}; do
        if grep -qE "\\$.*${property}" "${pom}"; then
            FOUND=true
        fi
    done
    if [ "${FOUND}" == "false" ]; then
        echo "${property}" > %FILE_UNUSED_PROPERTIES%
        echo "%ERROR_TEXT__UNUSED_PROPERTY%: ${property}"
    fi
done
